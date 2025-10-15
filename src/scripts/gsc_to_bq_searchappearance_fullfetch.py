# =================================================
# FILE: gsc_to_bq_searchappearance_fullfetch.py
# REV: 6.5.18
# PURPOSE: Full fetch SearchAppearance data from GSC to BigQuery
#          - Per-day & per-SearchType fetch
#          - Date column added (snapshot date per row)
#          - TargetEntity mapped from 00_01__gsc__searchappearance_enhancement_mapping
#          - unique_key = SHA256(SearchAppearance + '|' + TargetEntity + '|' + fetch_id)
#          - Placeholder rows for days with no GSC data
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
SITE_URL = "sc-domain:ghanadbashi.com"
BQ_PROJECT = 'ghanadbashi'
BQ_DATASET = 'seo_reports'
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
    Unique key for RAW rows is SHA256 of "searchappearance|Date|SearchType".
    """
    sa = (row.get('SearchAppearance') or '').strip().lower()
    date = str(row.get('Date') or '')
    stype = str(row.get('SearchType') or '')
    s = f"{sa}|{stype}|{date}"
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def ensure_table(table_name=BQ_TABLE_RAW):
    dataset_ref = bq_client.dataset(BQ_DATASET)
    table_ref = dataset_ref.table(table_name)
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {table_name} exists.", flush=True)
    except Exception:
        print(f"[INFO] Table {table_name} not found. Creating...", flush=True)
        if table_name == BQ_TABLE_RAW:
            schema = [
                bigquery.SchemaField("Date", "DATE"),
                bigquery.SchemaField("SearchAppearance", "STRING"),
                bigquery.SchemaField("SearchType", "STRING"),
                bigquery.SchemaField("Clicks", "INTEGER"),
                bigquery.SchemaField("Impressions", "INTEGER"),
                bigquery.SchemaField("CTR", "FLOAT"),
                bigquery.SchemaField("Position", "FLOAT"),
                bigquery.SchemaField("fetch_date", "DATE"),
                bigquery.SchemaField("fetch_id", "STRING"),
                bigquery.SchemaField("unique_key", "STRING"),
            ]
        else:  # Allocated table
            schema = [
                bigquery.SchemaField("Date", "DATE"),
                bigquery.SchemaField("SearchAppearance", "STRING"),
                bigquery.SchemaField("SearchType", "STRING"),
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

def get_existing_keys(table_name=BQ_TABLE_RAW):
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{table_name}`"
        df = bq_client.query(query).to_dataframe()
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys from {table_name}: {e}", flush=True)
        return set()

def fetch_mapping():
    """
    Read SearchAppearance -> Enhancement_Name mapping from BigQuery table
    """
    query = f"SELECT SearchAppearance, Enhancement_Name FROM `{BQ_PROJECT}.{BQ_DATASET}.00_01__gsc__searchappearance_enhancement_mapping`"
    try:
        df_map = bq_client.query(query).to_dataframe()
        return df_map
    except Exception as e:
        print(f"[ERROR] Failed to fetch mapping table: {e}", flush=True)
        return pd.DataFrame(columns=['SearchAppearance','Enhancement_Name'])

# =================================================
# BLOCK 5: FETCH DATA FROM GSC
# =================================================
def fetch_searchappearance_data(start_date, end_date):
    existing_keys = get_existing_keys(BQ_TABLE_RAW)
    mapping_df = fetch_mapping()
    print(f"[INFO] Fetching SearchAppearance data from {start_date} to {end_date}", flush=True)

    start_dt = datetime.strptime(start_date, "%Y-%m-%d").date()
    end_dt = datetime.strptime(end_date, "%Y-%m-%d").date()
    all_rows = []
    batch_reports = {}

    # default SearchTypes for GSC
    search_types = ["web", "image", "video"]

    for i in range((end_dt - start_dt).days + 1):
        cur_date = start_dt + timedelta(days=i)
        cur_date_str = cur_date.strftime("%Y-%m-%d")
        for stype in search_types:
            real_rows_count = 0
            placeholder_count = 0
            fetch_error = False

            request = {
                'startDate': cur_date_str,
                'endDate': cur_date_str,
                'dimensions': ['searchAppearance'],
                'rowLimit': ROW_LIMIT,
                'searchType': stype
            }
            try:
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
                rows = resp.get('rows', [])
            except Exception as e:
                print(f"[ERROR] Day {cur_date_str} fetch failed: {e}", flush=True)
                rows = []
                fetch_error = True

            if not rows:
                placeholder_sa = "__NO_APPEARANCE__"
                key_material = f"{placeholder_sa}|{stype}|{cur_date_str}"
                key = hashlib.sha256(key_material.encode()).hexdigest()
                if key not in existing_keys:
                    existing_keys.add(key)
                    all_rows.append({
                        'Date': cur_date_str,
                        'SearchAppearance': placeholder_sa,
                        'Clicks': 0,
                        'Impressions': 0,
                        'CTR': 0.0,
                        'Position': 0.0,
                        'SearchType': stype,
                        'fetch_date': FETCH_DATE,
                        'fetch_id': FETCH_ID,
                        'unique_key': key
                    })
                    placeholder_count = 1
            else:
                for r in rows:
                    sa = r['keys'][0]
                    clicks = r.get('clicks',0)
                    impressions = r.get('impressions',0)
                    ctr = r.get('ctr',0.0)
                    position = r.get('position',0.0)
                    key_material = f"{sa}|{stype}|{cur_date_str}"
                    key = hashlib.sha256(key_material.encode()).hexdigest()
                    if key not in existing_keys:
                        existing_keys.add(key)
                        all_rows.append({
                            'Date': cur_date_str,
                            'SearchAppearance': sa,
                            'SearchType': stype,
                            'Clicks': clicks,
                            'Impressions': impressions,
                            'CTR': ctr,
                            'Position': position,
                            'SearchType': stype,
                            'fetch_date': FETCH_DATE,
                            'fetch_id': FETCH_ID,
                            'unique_key': key
                        })
                        real_rows_count += 1

            # Batch report per SearchType
            batch_reports.setdefault(stype, []).append((cur_date_str, real_rows_count, placeholder_count, fetch_error))

    df_batch = pd.DataFrame(all_rows)
    if not df_batch.empty:
        df_batch['Date'] = pd.to_datetime(df_batch['Date']).dt.date
        df_batch['fetch_date'] = pd.to_datetime(df_batch['fetch_date']).dt.date

    # Print batch report per SearchType
    print("[INFO] Batch report per SearchType:", flush=True)
    for stype, reports in batch_reports.items():
        for day, real_rows, placeholders, fetch_error in reports:
            status = "OK" if not fetch_error else "FETCH_ERROR"
            print(f"  SearchType {stype} | Date {day} | New rows: {real_rows} | Placeholders: {placeholders} | Status: {status}", flush=True)

    total_new = len(df_batch)
    total_placeholders = sum(1 for reports in batch_reports.values() for r in reports if r[2]>0)
    total_errors = sum(1 for reports in batch_reports.values() for r in reports if r[3])
    print(f"[INFO] Total new rows: {total_new}. Total placeholders: {total_placeholders}. Total fetch errors: {total_errors}", flush=True)

    return df_batch, mapping_df

# =================================================
# BLOCK 6: UPLOAD FUNCTIONS
# =================================================
def upload_to_bq(df, table_name):
    if df is None or df.empty:
        print(f"[INFO] No new rows to insert into {table_name}.", flush=True)
        return

    numeric_cols = [c for c in df.columns if c in ['Clicks','Impressions','CTR','Position','Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc']]
    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    if 'unique_key' not in df.columns:
        df['unique_key'] = df.apply(lambda r: stable_key(r), axis=1)

    # remove duplicates
    full_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"
    try:
        existing_df = bq_client.query(f"SELECT unique_key FROM `{full_table_id}`").to_dataframe()
        existing_keys = set(existing_df['unique_key'].dropna().tolist())
    except Exception:
        existing_keys = set()
    df_filtered = df[~df['unique_key'].isin(existing_keys)].copy()
    skipped = len(df)-len(df_filtered)
    print(f"[INFO] {len(df_filtered)} new rows to insert, {skipped} duplicates skipped.", flush=True)

    if df_filtered.empty:
        return

    if DEBUG_MODE:
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
# BLOCK 7: ALLOCATION (Direct allocation)
# =================================================
def direct_allocation(df_raw, mapping_df):
    if df_raw is None or df_raw.empty:
        return pd.DataFrame(columns=['Date','SearchAppearance','TargetEntity','AllocationMethod','AllocationWeight','Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc','SearchType','fetch_id','unique_key'])
    
    df = df_raw.merge(mapping_df.rename(columns={'Enhancement_Name':'TargetEntity'}), on='SearchAppearance', how='left')
    df['AllocationMethod'] = 'direct'
    df['AllocationWeight'] = 1.0
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    df['CTR_alloc'] = df['Clicks_alloc']/df['Impressions_alloc'].replace(0,1)
    df['Position_alloc'] = df['Position']

    df['unique_key'] = df.apply(lambda r: hashlib.sha256(f"{r['SearchAppearance']}|{r['TargetEntity']}|{r['fetch_id']}".encode()).hexdigest(), axis=1)

    df_alloc = df[['Date','SearchAppearance','TargetEntity','AllocationMethod','AllocationWeight','Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc','SearchType','fetch_id','unique_key']]
    df_alloc.drop_duplicates(subset=['unique_key'], inplace=True)
    return df_alloc

# =================================================
# BLOCK 8: MAIN FUNCTION
# =================================================
def main():
    ensure_table(BQ_TABLE_RAW)
    ensure_table(BQ_TABLE_ALLOC)

    df_new, mapping_df = fetch_searchappearance_data(START_DATE, END_DATE)
    upload_to_bq(df_new, BQ_TABLE_RAW)

    if df_new is None or df_new.empty:
        query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}` WHERE Date >= '{START_DATE}' AND Date <= '{END_DATE}'"
        df_new = bq_client.query(query).to_dataframe()
        if not df_new.empty:
            df_new['Date'] = pd.to_datetime(df_new['Date']).dt.date

    df_alloc = direct_allocation(df_new, mapping_df)
    upload_to_bq(df_alloc, BQ_TABLE_ALLOC)

    print("[INFO] Finished processing SearchAppearance data.", flush=True)

# =================================================
# BLOCK 9: SCRIPT EXECUTION
# =================================================
if __name__ == "__main__":
    main()
