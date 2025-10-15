# =================================================
# FILE: gsc_to_bq_searchappearance_fullfetch.py
# REV: 2
# PURPOSE: Full fetch SearchAppearance data from GSC to BigQuery
#          - Per-day fetch loop (start_date .. end_date)
#          - Date column added (snapshot date per row)
#          - unique_key = SHA256(SearchAppearance + '|' + Date)
#          - Placeholder rows for days with no GSC data:
#              SearchAppearance="__NO_APPEARANCE__", metrics=0
#          - Per-day (Batch) reporting and final summary
# =================================================

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import argparse
import warnings
import uuid

# =================================================
# BLOCK 1: CONFIGURATION & ARGUMENT PARSING
# =================================================
# NOTE: Project/dataset adjusted to use testing dataset per workspace policy
SITE_URL = "sc-domain:ghanadbashi.com"  # keep original site (modify if needed)
BQ_PROJECT = 'ghanadbashi'                  # <<-- updated to ghanadbashi per chat context
BQ_DATASET = 'seo_reports'                  # <<-- dataset
BQ_TABLE_RAW = 'ghanadbashi__gsc__raw_domain_data_searchappearance'
BQ_TABLE_ALLOC = 'ghanadbashi__gsc__allocated_searchappearance'
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds in case of timeout

parser = argparse.ArgumentParser(description="GSC SearchAppearance to BigQuery Full Fetch")
parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD for full fetch (inclusive)")
parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD for full fetch (inclusive)")
parser.add_argument("--debug", action="store_true", help="Enable debug mode (skip BQ insert)")
parser.add_argument("--csv-test", type=str, help=argparse.SUPPRESS)
args = parser.parse_args()

# Default behavior: snapshot the latest fully-published GSC day.
# Use 4 days ago as the default END_DATE (to avoid GSC delay).
if args.start_date and args.end_date:
    START_DATE = args.start_date
    END_DATE = args.end_date
else:
    END_DATE = args.end_date or (datetime.utcnow() - timedelta(days=4)).strftime('%Y-%m-%d')
    START_DATE = args.start_date or END_DATE

DEBUG_MODE = args.debug

# =================================================
# BLOCK 2: FETCH METADATA
# =================================================
FETCH_DATE = datetime.utcnow().strftime('%Y-%m-%d')
FETCH_ID = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid.uuid4().hex[:6]}"

# =================================================
# BLOCK 3: CREDENTIALS & SERVICE CLIENTS
# =================================================
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(SERVICE_ACCOUNT_FILE, "r") as f:
    sa_info = json.load(f)
credentials = service_account.Credentials.from_service_account_info(sa_info)
service = build('searchconsole', 'v1', credentials=credentials)
bq_client = bigquery.Client(credentials=credentials, project=BQ_PROJECT)

# =================================================
# BLOCK 4: HELPER FUNCTIONS (Keys & Table Setup)
# =================================================
def stable_key(row):
    """
    Unique key for RAW rows is SHA256 of "searchappearance|date".
    Date must be 'YYYY-MM-DD' string.
    """
    sa = (row.get('SearchAppearance') or '').strip().lower()
    date = (row.get('Date') or row.get('fetch_date') or '').strip()
    s = f"{sa}|{date}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def ensure_table(table_name=BQ_TABLE_RAW):
    """
    Ensure table exists; if not, create with required schema.
    RAW table now includes 'Date' (snapshot date per row).
    """
    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_name)
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {table_name} exists.", flush=True)
    except Exception:
        print(f"[INFO] Table {table_name} not found. Creating...", flush=True)
        if table_name == BQ_TABLE_RAW:
            schema = [
                bigquery.SchemaField("Date", "DATE"),       # NEW: snapshot date (used in unique_key)
                bigquery.SchemaField("SearchAppearance", "STRING"),
                bigquery.SchemaField("Clicks", "INTEGER"),
                bigquery.SchemaField("Impressions", "INTEGER"),
                bigquery.SchemaField("CTR", "FLOAT"),
                bigquery.SchemaField("Position", "FLOAT"),
                bigquery.SchemaField("fetch_date", "DATE"), # run date of this script
                bigquery.SchemaField("fetch_id", "STRING"),
                bigquery.SchemaField("unique_key", "STRING"),
            ]
        else:  # Allocated table
            schema = [
                bigquery.SchemaField("SearchAppearance", "STRING"),
                bigquery.SchemaField("TargetEntity", "STRING"),
                bigquery.SchemaField("AllocationMethod", "STRING"),
                bigquery.SchemaField("AllocationWeight", "FLOAT"),
                bigquery.SchemaField("Clicks_alloc", "FLOAT"),
                bigquery.SchemaField("Impressions_alloc", "FLOAT"),
                bigquery.SchemaField("CTR_alloc", "FLOAT"),
                bigquery.SchemaField("Position_alloc", "FLOAT"),
                bigquery.SchemaField("fetch_id", "STRING"),
                bigquery.SchemaField("unique_key", "STRING"),
            ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["Date", "SearchAppearance"]
        bq_client.create_table(table)
        print(f"[INFO] Table {table_name} created.", flush=True)

def get_existing_keys():
    """
    Retrieve existing unique_key values from RAW table to prevent duplicates.
    Returns a set of string keys.
    """
    try:
        full_table = f"{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}"
        query = f"SELECT unique_key FROM `{full_table}`"
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        except Exception:
            df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# =================================================
# BLOCK 5: FETCH DATA FROM GSC (Per-day loop, placeholders, batch reports)
# =================================================

# ---------------------------
# BATCH & PLACEHOLDER LOGIC
# For each day in the requested range we:
#  - Query GSC for that single day.
#  - If GSC returns rows -> insert them with Date = that day.
#  - If GSC returns no rows -> insert a placeholder row:
#       SearchAppearance = "__NO_APPEARANCE__", metrics = 0
#  - Each day is considered a separate Batch (BatchNo).
#  - We log per-batch counts and produce a final aggregate summary report.
# ---------------------------

def fetch_searchappearance_data(start_date, end_date):
    existing_keys = get_existing_keys()
    print(f"[INFO] Fetching SearchAppearance data from {start_date} to {end_date}", flush=True)

    # prepare per-day range
    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    if end_dt < start_dt:
        raise ValueError("end_date must be >= start_date")

    all_rows = []
    batch_reports = []  # (BatchNo, Date, real_rows, placeholders, fetch_error_flag)

    total_days = (end_dt - start_dt).days + 1
    print(f"[INFO] Processing {total_days} day(s).", flush=True)

    for i in range((end_dt - start_dt).days + 1):
        cur_date = start_dt + timedelta(days=i)
        cur_date_str = cur_date.strftime("%Y-%m-%d")
        batch_no = i + 1
        real_rows_count = 0
        placeholder_count = 0
        fetch_error = False

        request = {
            'startDate': cur_date_str,
            'endDate': cur_date_str,
            'dimensions': ['searchAppearance'],
            'rowLimit': ROW_LIMIT,
        }
        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            rows = resp.get('rows', [])
        except Exception as e:
            print(f"[ERROR] Day {cur_date_str} fetch failed: {e}", flush=True)
            rows = []
            fetch_error = True

        if not rows:
            # Create placeholder row for days with no data (or fetch error)
            placeholder_sa = "__NO_APPEARANCE__"
            placeholder_key = hashlib.sha256(f"{placeholder_sa}|{cur_date_str}".encode()).hexdigest()
            if placeholder_key not in existing_keys:
                existing_keys.add(placeholder_key)
                all_rows.append({
                    'Date': cur_date_str,
                    'SearchAppearance': placeholder_sa,
                    'Clicks': 0,
                    'Impressions': 0,
                    'CTR': 0.0,
                    'Position': 0.0,
                    'fetch_date': FETCH_DATE,
                    'fetch_id': FETCH_ID,
                    'unique_key': placeholder_key
                })
                placeholder_count = 1
                print(f"[INFO] Day {cur_date_str}: No rows from GSC -> placeholder inserted.", flush=True)
            else:
                print(f"[INFO] Day {cur_date_str}: Placeholder already exists, skipped.", flush=True)
        else:
            # Process returned rows
            for r in rows:
                sa = r['keys'][0]
                clicks = r.get('clicks', 0)
                impressions = r.get('impressions', 0)
                ctr = r.get('ctr', 0.0)
                position = r.get('position', 0.0)
                # stable key includes date so snapshots are per-day
                key_material = f"{sa.strip().lower()}|{cur_date_str}"
                key = hashlib.sha256(key_material.encode()).hexdigest()
                if key not in existing_keys:
                    existing_keys.add(key)
                    all_rows.append({
                        'Date': cur_date_str,
                        'SearchAppearance': sa,
                        'Clicks': clicks,
                        'Impressions': impressions,
                        'CTR': ctr,
                        'Position': position,
                        'fetch_date': FETCH_DATE,
                        'fetch_id': FETCH_ID,
                        'unique_key': key
                    })
                    real_rows_count += 1
                else:
                    # existing snapshot for this SearchAppearance & Date -> skip
                    pass
            print(f"[INFO] Day {cur_date_str}: Retrieved {len(rows)} rows from GSC, {real_rows_count} new.", flush=True)

        batch_reports.append((batch_no, cur_date_str, real_rows_count, placeholder_count, fetch_error))

    # Build DataFrame
    if all_rows:
        df_batch = pd.DataFrame(all_rows, columns=[
            'Date','SearchAppearance','Clicks','Impressions','CTR','Position','fetch_date','fetch_id','unique_key'
        ])
        df_batch['Date'] = pd.to_datetime(df_batch['Date']).dt.date
        df_batch['fetch_date'] = pd.to_datetime(df_batch['fetch_date']).dt.date
    else:
        df_batch = pd.DataFrame(columns=[
            'Date','SearchAppearance','Clicks','Impressions','CTR','Position','fetch_date','fetch_id','unique_key'
        ])

    # print per-batch report
    print("[INFO] Per-day batch report:", flush=True)
    for br in batch_reports:
        bn, day, real_rows, placeholders, fetch_error = br
        status = "OK" if not fetch_error else "FETCH_ERROR"
        print(f"  Batch {bn} | Date {day} | New rows: {real_rows} | Placeholders: {placeholders} | Status: {status}", flush=True)

    # aggregate summary
    total_new = len(df_batch)
    total_placeholders = sum(1 for br in batch_reports if br[3] > 0)
    total_days_with_error = sum(1 for br in batch_reports if br[4])
    print(f"[INFO] Total new rows: {total_new}. Days with placeholders: {total_placeholders}. Days with fetch errors: {total_days_with_error}", flush=True)

    return df_batch

# =================================================
# BLOCK 6: UPLOAD FUNCTIONS (Updated with Duplicate Prevention)
# =================================================
def upload_to_bq(df, table_name):
    """
    Upload DataFrame to BigQuery with duplicate prevention on unique_key.
    Expects Date column for RAW uploads.
    """
    if df is None or df.empty:
        print(f"[INFO] No new rows to insert into {table_name}.", flush=True)
        return

    # --- Ensure numeric columns are numeric ---
    if 'Clicks' in df.columns:
        numeric_cols = ['Clicks','Impressions','CTR','Position']
    elif 'Clicks_alloc' in df.columns:
        numeric_cols = ['Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc']
    else:
        numeric_cols = []

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # --- Ensure unique_key exists ---
    if 'unique_key' not in df.columns:
        print(f"[WARNING] No 'unique_key' column found in {table_name}. Skipping duplicate check.", flush=True)
        df_filtered = df.copy()
    else:
        # --- Get existing keys from BigQuery for the target table ---
        full_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
        query_existing = f"SELECT unique_key FROM `{full_table_id}`"
        try:
            existing_df = bq_client.query(query_existing).to_dataframe()
            existing_keys = set(existing_df['unique_key'].dropna().tolist())
            print(f"[INFO] Retrieved {len(existing_keys)} existing keys from {table_name}.", flush=True)
        except Exception as e:
            print(f"[WARNING] Could not retrieve existing keys: {e}", flush=True)
            existing_keys = set()

        # --- Filter out duplicates ---
        df_filtered = df[~df['unique_key'].isin(existing_keys)].copy()
        skipped = len(df) - len(df_filtered)
        print(f"[INFO] {len(df_filtered)} new rows to insert, {skipped} duplicates skipped.", flush=True)

    if df_filtered.empty:
        print(f"[INFO] No new rows to insert after duplicate filtering for {table_name}.", flush=True)
        return

    # --- Upload filtered data ---
    full_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    if DEBUG_MODE:
        # In debug mode, optionally write CSV locally for inspection
        csv_file = args.csv_test or f"gsc_searchappearance_test_{FETCH_ID}.csv"
        df_filtered.to_csv(csv_file, index=False)
        print(f"[DEBUG] Debug mode ON. Wrote {len(df_filtered)} rows to {csv_file}. Not uploading to BigQuery.", flush=True)
        return

    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq_client.load_table_from_dataframe(df_filtered, full_table_id, job_config=job_config).result()
        print(f"[INFO] Inserted {len(df_filtered)} rows to {full_table_id}.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)

# =================================================
# BLOCK 7: ALLOCATION FUNCTIONS (DIRECT / SAMPLE / PROPORTIONAL)
# =================================================
def direct_allocation(df_raw, mapping_df=None):
    """
    Direct allocation of SearchAppearance data to TargetEntity with deduplication.
    Ensures each unique (SearchAppearance, TargetEntity, fetch_id) combination appears once.
    Assumes df_raw contains Date/fetch_id/unique_key from RAW.
    """
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=[
            'SearchAppearance', 'TargetEntity', 'AllocationMethod', 'AllocationWeight',
            'Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc','fetch_id','unique_key'
        ])

    # اگر مپینگ ارسال نشده باشه، از خود SearchAppearance استفاده می‌کنیم
    if mapping_df is None or mapping_df.empty:
        mapping_df = pd.DataFrame({
            'SearchAppearance': df_raw['SearchAppearance'].unique(),
            'TargetEntity': df_raw['SearchAppearance'].unique()
        })

    # اتصال داده خام به مپینگ
    df = df_raw.merge(mapping_df, on='SearchAppearance', how='left')

    # محاسبه شاخص‌های تخصیص
    df['AllocationMethod'] = 'direct'
    df['AllocationWeight'] = 1.0
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    # Avoid divide-by-zero
    df['CTR_alloc'] = df['Clicks_alloc'] / df['Impressions_alloc'].replace(0, 1)
    df['Position_alloc'] = df['Position']

    # افزودن شناسه فچ و کلید یکتا (برای allocated ما می‌سازیم که ترکیبی از SA|TargetEntity|fetch_id است)
    df['fetch_id'] = df['fetch_id']
    df['unique_key'] = df.apply(
        lambda r: hashlib.sha256(
            f"{r.get('SearchAppearance','')}|{r.get('TargetEntity','')}|{r.get('fetch_id','')}".encode()
        ).hexdigest(),
        axis=1
    )

    # حذف رکوردهای تکراری بر اساس unique_key
    before_count = len(df)
    df.drop_duplicates(subset=['unique_key'], inplace=True)
    after_count = len(df)
    if after_count < before_count:
        print(f"[INFO] Removed {before_count - after_count} duplicate rows from allocation.", flush=True)

    # بازگرداندن ستون‌های نهایی مطابق با جدول Allocated
    df_alloc = df[['SearchAppearance', 'TargetEntity', 'AllocationMethod', 'AllocationWeight',
                   'Clicks_alloc', 'Impressions_alloc', 'CTR_alloc', 'Position_alloc',
                   'fetch_id', 'unique_key']]

    return df_alloc

def sample_driven_allocation(df_raw, mapping_df=None):
    # Placeholder for future sample-driven allocation logic
    return direct_allocation(df_raw, mapping_df)  # fallback to direct for now

def proportional_allocation(df_raw, mapping_df=None):
    # Placeholder for future proportional allocation logic
    return direct_allocation(df_raw, mapping_df)  # fallback to direct for now

# =================================================
# BLOCK 8: MAIN FUNCTION
# =================================================
def main():
    # اطمینان از وجود جدول‌ها
    ensure_table(BQ_TABLE_RAW)
    ensure_table(BQ_TABLE_ALLOC)

    # فچ داده‌ها از GSC (per-day, placeholders included)
    df_new = fetch_searchappearance_data(START_DATE, END_DATE)
    # آپلود RAW جدید
    upload_to_bq(df_new, BQ_TABLE_RAW)

    # اگر داده جدیدی نبود (ولی ممکن است placeholderها هم بوده باشند) و لازم باشه داده برای allocation بارگذاری شود:
    if df_new is None or df_new.empty:
        print("[INFO] No new Raw rows found in fetch. Attempting to load existing Raw data for allocation...", flush=True)
        query = f"""
            SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}`
            WHERE Date >= '{START_DATE}' AND Date <= '{END_DATE}'
        """
        try:
            df_new = bq_client.query(query).to_dataframe()
            if not df_new.empty:
                df_new['Date'] = pd.to_datetime(df_new['Date']).dt.date
            print(f"[INFO] Loaded {len(df_new)} rows from existing RAW for allocation.", flush=True)
        except Exception as e:
            print(f"[ERROR] Failed to load existing RAW data for allocation: {e}", flush=True)
            df_new = pd.DataFrame()

    # اعمال روش تخصیص (فعلاً Direct فعال است)
    df_alloc = direct_allocation(df_new)

    # آپلود نتایج تخصیص‌یافته به جدول Allocated
    upload_to_bq(df_alloc, BQ_TABLE_ALLOC)

    print("[INFO] Finished processing SearchAppearance data.", flush=True)

# =================================================
# BLOCK 9: SCRIPT EXECUTION
# =================================================
if __name__ == "__main__":
    main()
