# =================================================
# FILE: gsc_to_bq_searchappearance_fullfetch.py
# REV: 0
# PURPOSE: Full fetch SearchAppearance data from GSC to BigQuery
#          + allocation applied on new or existing Raw data
#          + Direct / Sample-driven / Proportional allocation base
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
parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD for full fetch")
parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Enable debug mode (skip BQ insert)")
parser.add_argument("--csv-test", type=str, help=argparse.SUPPRESS)
args = parser.parse_args()

START_DATE = args.start_date or (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
END_DATE = args.end_date or datetime.utcnow().strftime('%Y-%m-%d')
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
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)

# =================================================
# BLOCK 4: HELPER FUNCTIONS (Keys & Table Setup)
# =================================================
def stable_key(row):
    sa = (row.get('SearchAppearance') or '').strip().lower()
    det_tuple = (sa,)
    s = "|".join(det_tuple)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

def ensure_table(table_name=BQ_TABLE_RAW):
    try:
        bq_client.get_table(bq_client.dataset(BQ_DATASET).table(table_name))
        print(f"[INFO] Table {table_name} exists.", flush=True)
    except:
        print(f"[INFO] Table {table_name} not found. Creating...", flush=True)
        if table_name == BQ_TABLE_RAW:
            schema = [
                bigquery.SchemaField("SearchAppearance", "STRING"),
                bigquery.SchemaField("Clicks", "INTEGER"),
                bigquery.SchemaField("Impressions", "INTEGER"),
                bigquery.SchemaField("CTR", "FLOAT"),
                bigquery.SchemaField("Position", "FLOAT"),
                bigquery.SchemaField("unique_key", "STRING"),
                bigquery.SchemaField("fetch_date", "DATE"),
                bigquery.SchemaField("fetch_id", "STRING"),
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
        table = bigquery.Table(bq_client.dataset(BQ_DATASET).table(table_name), schema=schema)
        table.clustering_fields = ["SearchAppearance"]
        bq_client.create_table(table)
        print(f"[INFO] Table {table_name} created.", flush=True)

def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}`"
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
# BLOCK 5: FETCH DATA FROM GSC
# =================================================
def fetch_searchappearance_data(start_date, end_date):
    existing_keys = get_existing_keys()
    print(f"[INFO] Fetching SearchAppearance data from {start_date} to {end_date}", flush=True)
    request = {
        'startDate': start_date,
        'endDate': end_date,
        'dimensions': ['searchAppearance'],
        'rowLimit': ROW_LIMIT,
    }
    try:
        resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
    except Exception as e:
        print(f"[ERROR] Failed fetching SearchAppearance data: {e}", flush=True)
        return pd.DataFrame()

    rows = resp.get('rows', [])
    batch_new_rows = []
    for r in rows:
        sa = r['keys'][0]
        clicks = r.get('clicks', 0)
        impressions = r.get('impressions', 0)
        ctr = r.get('ctr', 0.0)
        position = r.get('position', 0.0)
        key = stable_key({'SearchAppearance': sa})
        if key not in existing_keys:
            existing_keys.add(key)
            batch_new_rows.append([sa, clicks, impressions, ctr, position, key, FETCH_DATE, FETCH_ID])
    df_batch = pd.DataFrame(batch_new_rows, columns=[
        'SearchAppearance','Clicks','Impressions','CTR','Position','unique_key','fetch_date','fetch_id'
    ])
    df_batch['fetch_date'] = pd.to_datetime(df_batch['fetch_date']).dt.date
    print(f"[INFO] {len(df_batch)} new rows to insert. {len(rows)-len(df_batch)} duplicate rows skipped.", flush=True)
    return df_batch

# =================================================
# BLOCK 6: UPLOAD FUNCTIONS
# =================================================
# =================================================
# BLOCK 6: UPLOAD FUNCTIONS (Updated for 6.5.15)
# =================================================
def upload_to_bq(df, table_name):
    if df.empty:
        print(f"[INFO] No new rows to insert into {table_name}.", flush=True)
        return

    # --- Determine schema type and numeric columns ---
    if 'Clicks' in df.columns:
        numeric_cols = ['Clicks','Impressions','CTR','Position']
    elif 'Clicks_alloc' in df.columns:
        numeric_cols = ['Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc']
    else:
        numeric_cols = []

    for col in numeric_cols:
        df[col] = pd.to_numeric(df[col], errors='coerce').fillna(0)

    # --- Ensure fully-qualified table_id ---
    full_table_id = f"{BQ_PROJECT}.{BQ_DATASET}.{table_name}"

    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        bq_client.load_table_from_dataframe(df, full_table_id, job_config=job_config).result()
        print(f"[INFO] Inserted {len(df)} rows to {full_table_id}.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)


# =================================================
# BLOCK 7: ALLOCATION FUNCTIONS (DIRECT / SAMPLE / PROPORTIONAL)
# =================================================
def direct_allocation(df_raw, mapping_df=None):
    if mapping_df is None or mapping_df.empty:
        mapping_df = pd.DataFrame({
            'SearchAppearance': df_raw['SearchAppearance'],
            'TargetEntity': df_raw['SearchAppearance']
        })
    df = df_raw.merge(mapping_df, on='SearchAppearance', how='left')
    df['AllocationMethod'] = 'direct'
    df['AllocationWeight'] = 1.0
    df['Clicks_alloc'] = df['Clicks'] * df['AllocationWeight']
    df['Impressions_alloc'] = df['Impressions'] * df['AllocationWeight']
    df['CTR_alloc'] = df['Clicks_alloc'] / df['Impressions_alloc'].replace(0,1)
    df['Position_alloc'] = df['Position']
    df['fetch_id'] = df_raw['fetch_id']
    df['unique_key'] = df.apply(lambda r: hashlib.sha256(f"{r['SearchAppearance']}|{r.get('TargetEntity','')}|{r['fetch_id']}".encode()).hexdigest(), axis=1)
    return df

def sample_driven_allocation(df_raw, mapping_df=None):
    # Placeholder for future sample-driven allocation logic
    return direct_allocation(df_raw, mapping_df)  # fallback to direct for now

def proportional_allocation(df_raw, mapping_df=None):
    # Placeholder for future proportional allocation logic
    return direct_allocation(df_raw, mapping_df)  # fallback to direct for now

# =================================================
# BLOCK 8: MAIN FUNCTION (Updated for 6.5.13)
# =================================================
# =================================================
# BLOCK 8: MAIN FUNCTION (Updated for 6.5.15)
# =================================================
def main():
    ensure_table(BQ_TABLE_RAW)
    df_new = fetch_searchappearance_data(START_DATE, END_DATE)
    upload_to_bq(df_new, BQ_TABLE_RAW)

    # --- If no new rows, fetch existing Raw data for allocation ---
    if df_new.empty:
        print("[INFO] No new Raw rows, fetching existing Raw data for allocation...")
        query = f"SELECT * FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE_RAW}` WHERE fetch_date >= '{START_DATE}'"
        df_new = bq_client.query(query).to_dataframe()
        df_new['fetch_date'] = pd.to_datetime(df_new['fetch_date']).dt.date

    ensure_table(BQ_TABLE_ALLOC)

    # --- Apply allocation (currently only direct active, others as placeholder) ---
    df_alloc = direct_allocation(df_new)

    # --- Keep only columns expected by Allocated table schema ---
    df_alloc = df_alloc[['SearchAppearance','TargetEntity','AllocationMethod','AllocationWeight',
                         'Clicks_alloc','Impressions_alloc','CTR_alloc','Position_alloc',
                         'fetch_id','unique_key']]

    upload_to_bq(df_alloc, BQ_TABLE_ALLOC)

    print("[INFO] Finished processing SearchAppearance data.", flush=True)


# =================================================
# BLOCK 9: SCRIPT EXECUTION
# =================================================
if __name__ == "__main__":
    main()
