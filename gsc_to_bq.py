# =================================================
# FILE: gsc_to_bq_rev4_incremental.py
# REV: 4
# PURPOSE: Incremental GSC to BigQuery loader with duplicate prevention
# =================================================

from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from google.cloud import bigquery
import pandas as pd
from datetime import datetime, timedelta
import hashlib
import time
import os
import json
import sys
import argparse
import warnings

# ---------- CONFIG ----------
SITE_URL = 'https://ghanadbashi.com/'
BQ_PROJECT = 'ghanadbashi'
BQ_DATASET = 'seo_reports'
BQ_TABLE = 'ghanadbashi__gsc__raw_data'
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds in case of timeout

# ---------- ARGUMENT PARSER ----------
parser = argparse.ArgumentParser(description="GSC to BigQuery Incremental Loader")
parser.add_argument("--start-date", type=str, help="Start date YYYY-MM-DD for incremental load")
parser.add_argument("--end-date", type=str, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Enable debug mode (skip BQ insert)")
args = parser.parse_args()

START_DATE = args.start_date or (datetime.utcnow() - timedelta(days=3)).strftime('%Y-%m-%d')
END_DATE = args.end_date or datetime.utcnow().strftime('%Y-%m-%d')
DEBUG_MODE = args.debug

# ---------- CREDENTIALS ----------
service_account_file = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
with open(service_account_file, "r") as f:
    sa_info = json.load(f)

credentials = service_account.Credentials.from_service_account_info(sa_info)

# Build Search Console service
service = build('searchconsole', 'v1', credentials=credentials)

# --- Check access ---
try:
    response = service.sites().get(siteUrl=SITE_URL).execute()
    print(f"✅ Service Account has access to {SITE_URL}", flush=True)
except Exception as e:
    print(f"❌ Service Account does NOT have access to {SITE_URL}", flush=True)
    print("Error details:", e)
    sys.exit(1)

# ---------- BIGQUERY CLIENT ----------
bq_client = bigquery.Client(credentials=credentials, project=credentials.project_id)
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- ENSURE TABLE EXISTS ----------
def ensure_table():
    try:
        bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.", flush=True)
    except:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...", flush=True)
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["Date", "Query"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created.", flush=True)

# ---------- HELPER: create unique key ----------
def stable_key(row):
    query = (row.get('Query') or '').strip().lower()
    page = (row.get('Page') or '').strip().lower().rstrip('/')
    date_raw = row.get('Date')
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    det_tuple = (query, page, date)
    s = "|".join(det_tuple)
    return hashlib.sha256(s.encode('utf-8')).hexdigest()

# ---------- FETCH EXISTING KEYS FROM BIGQUERY ----------
def get_existing_keys():
    try:
        query = f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}`"

        # ایجاد کلاینت BigQuery Storage
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            use_bqstorage = True
        except Exception:
            bqstorage_client = None
            use_bqstorage = False

        # فراخوانی Query و تبدیل به DataFrame با چک Storage
        if use_bqstorage:
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        else:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = bq_client.query(query).to_dataframe()

        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery.", flush=True)
        return set(df['unique_key'].astype(str).tolist())

    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return
    df['Date'] = pd.to_datetime(df['Date'])
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery")
        return
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date):
    all_rows = []
    start_row = 0
    existing_keys = get_existing_keys()
    batch_index = 1

    while True:
        request = {
            'startDate': start_date,
            'endDate': end_date,
            'dimensions': ['date','query','page'],
            'rowLimit': ROW_LIMIT,
            'startRow': start_row
        }

        try:
            resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
        except Exception as e:
            print(f"[ERROR] Timeout or error: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
            time.sleep(RETRY_DELAY)
            continue

        rows = resp.get('rows', [])
        if not rows:
            print("[INFO] No more rows returned from GSC.", flush=True)
            break

        batch_count = 0
        batch_new_rows = []
        for r in rows:
            date = r['keys'][0]
            query_text = r['keys'][1]
            page = r['keys'][2]
            clicks = r.get('clicks',0)
            impressions = r.get('impressions',0)
            ctr = r.get('ctr',0)
            position = r.get('position',0)
            key = stable_key({
                'Query': query_text,
                'Page': page,
                'Date': date
            })
            if key not in existing_keys:
                existing_keys.add(key)
                batch_new_rows.append([date, query_text, page, clicks, impressions, ctr, position, key])
                batch_count += 1

        print(f"[INFO] Batch {batch_index}: Fetched {len(rows)} rows, {batch_count} new rows.", flush=True)
        if batch_new_rows:
            df_batch = pd.DataFrame(batch_new_rows, columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key'])
            upload_to_bq(df_batch)

        batch_index += 1
        if len(rows) < ROW_LIMIT:
            break
        start_row += len(rows)

    return pd.DataFrame(all_rows, columns=['Date','Query','Page','Clicks','Impressions','CTR','Position','unique_key'])

# ---------- MAIN ----------
if __name__ == "__main__":
    ensure_table()
    df = fetch_gsc_data(START_DATE, END_DATE)

    # اگر insert_rows_to_bigquery در جای دیگری اجرا می‌شود، همان‌جا این را اضافه کنید
    total_new_rows = 0
    if 'new_rows' in locals():
        total_new_rows = len(new_rows)  # یا جمع همه batch ها
    print(f"[INFO] Finished fetching all data. Total new rows: {total_new_rows}", flush=True)
