#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: upload_gsc_enhancements.py
# Revision: Rev.35 — Improved merge, dedupe, metrics handling, mapping, and logging
# Purpose: Parse GSC Enhancement XLSX exports, build per-URL raw enhancements table, handle metrics safely, and load to BigQuery with dedupe.
# ============================================================

import os
import re
import argparse
import hashlib
from datetime import datetime, date
from uuid import uuid4
import warnings
import pandas as pd
import glob
from google.cloud import bigquery

# نادیده گرفتن هشدارهای مربوط به Workbook بدون style پیش‌فرض
warnings.filterwarnings(
    "ignore",
    category=UserWarning,
    module="openpyxl.styles.stylesheet"
)

# =================================================
# BLOCK 1: CONFIG / CONSTANTS
# =================================================
PROJECT_ID = "ghanadbashi"
DATASET_ID = "seo_reports"
TABLE_ID = "ghanadbashi__gsc__raw_enhancements"
MAPPING_TABLE = f"{PROJECT_ID}.{DATASET_ID}. ghanadbashi__gsc__searchappearance_enhancement_mapping"
GITHUB_LOCAL_PATH_DEFAULT = "gsc_enhancements"

FINAL_COLUMNS = [
    "site",
    "date",
    "enhancement_name",
    "appearance_type",
    "page",
    "item_name",
    "issue_name",
    "last_crawled",
    "status",
    "fetch_id",
    "fetch_date",
    "source_file",
    "unique_key",
    "impressions",
    "clicks",
    "ctr",
    "position"
]

DEBUG_MODE = False

# =================================================
# BLOCK 2: ARGPARSE
# =================================================
parser = argparse.ArgumentParser(description="Upload GSC Enhancements Data to BigQuery")
parser.add_argument("--start-date", type=str)
parser.add_argument("--end-date", type=str)
parser.add_argument("--local-path", type=str, default=GITHUB_LOCAL_PATH_DEFAULT)
parser.add_argument("--csv-test", type=str, default=None)
parser.add_argument("--debug", action="store_true")
args = parser.parse_args()
start_date = args.start_date
end_date = args.end_date
enhancement_folder = args.local_path
csv_test_path = args.csv_test
DEBUG_MODE = args.debug

# =================================================
# BLOCK 3: CLIENTS & UTIL
# =================================================
bq_client = bigquery.Client(project=PROJECT_ID)

def now_fetch_id():
    return datetime.utcnow().strftime("%Y%m%d_%H%M%S") + "_" + uuid4().hex[:8]

FETCH_ID = now_fetch_id()
FETCH_DATE = datetime.utcnow().date()

# =================================================
# BLOCK 4: PARSING HELPERS
# =================================================
def parse_filename_metadata(filename):
    base = os.path.basename(filename)
    name = re.sub(r'\.xlsx$', '', base, flags=re.IGNORECASE)
    m = re.search(r'^(?P<prefix>.+)-(?P<date>\d{4}-\d{2}-\d{2})$', name)
    if not m:
        return {"site_raw": None, "site": None, "enhancement_name": None, "date": None, "status_hint": None, "source_file": base}
    prefix = m.group("prefix")
    date_str = m.group("date")
    try:
        dt = datetime.strptime(date_str, "%Y-%m-%d").date()
    except Exception:
        dt = None
    parts = prefix.split("-")
    site_raw = parts[0] if parts else ""
    status_hint = None
    last_token = parts[-1].strip().lower() if len(parts) >= 2 else ""
    if last_token in ("valid", "invalid", "valid_items", "invalid_items"):
        status_hint = "Valid" if "valid" in last_token else "Invalid"
        enh_tokens = parts[1:-1]
    else:
        enh_tokens = parts[1:]
    enhancement_name = "-".join(enh_tokens).strip()
    site = site_raw
    if site and site.lower().endswith(".com"):
        site = site[:-4]
    else:
        site = re.sub(r'\.[a-z]{2,}$', '', site)
    return {"site_raw": site_raw, "site": site, "enhancement_name": enhancement_name, "date": dt, "status_hint": status_hint, "source_file": base}

# =================================================
# BLOCK 5: EXCEL PARSE (Details + Chart)
# =================================================
def _normalize_columns(cols):
    normalized = []
    for c in cols:
        if not isinstance(c, str):
            c = str(c)
        s = c.strip().lower().replace("\n", " ").replace("\r", " ")
        s = re.sub(r'\s+', '_', s)
        normalized.append(s)
    return normalized

def parse_excel_file(file_path):
    try:
        xls = pd.ExcelFile(file_path)
    except Exception as e:
        print(f"[WARN] Cannot open {file_path}: {e}")
        return pd.DataFrame(), pd.DataFrame()

    details_frames = []
    metrics_frames = []

    # Table sheet
    if "Table" in xls.sheet_names:
        try:
            df_table = pd.read_excel(xls, sheet_name="Table")
            if "Item name" in df_table.columns:
                df_table['item_name'] = df_table['Item name']
            for col in ['URL', 'Page', 'page']:
                if col in df_table.columns:
                    df_table['url'] = df_table[col]
                    break
            else:
                df_table['url'] = None
            details_frames.append(df_table)
        except Exception as e:
            print(f"[WARN] Failed to read Table sheet: {e}")

    # Chart sheet
    if "Chart" in xls.sheet_names:
        try:
            df_chart = pd.read_excel(xls, sheet_name="Chart")
            df_chart.columns = df_chart.columns.str.strip().str.lower()
            required_cols = ["page","appearance_type","impressions","clicks","ctr","position","item_name","issue_name","last_crawled","status"]
            for col in required_cols:
                if col not in df_chart.columns:
                    df_chart[col] = None
            # Metrics conversion
            for col in ["impressions","clicks","position"]:
                if col in df_chart.columns:
                    df_chart[col] = df_chart[col].replace('None', 0)
                    df_chart[col] = pd.to_numeric(df_chart[col], errors="coerce").fillna(0).astype(int)
            # Handle CTR percentage strings
            if "ctr" in df_chart.columns:
                df_chart["ctr"] = df_chart["ctr"].replace('None', 0)
                df_chart["ctr"] = pd.to_numeric(df_chart["ctr"].astype(str).str.replace("%",""), errors="coerce").fillna(0)/100

            metrics_frames.append(df_chart)
        except Exception as e:
            print(f"[WARN] Failed to read Chart sheet: {e}")

    details_df = pd.concat(details_frames, ignore_index=True) if details_frames else pd.DataFrame()
    metrics_df = pd.concat(metrics_frames, ignore_index=True) if metrics_frames else pd.DataFrame()

    # Merge safely on url + item_name
    if not details_df.empty and not metrics_df.empty:
        details_df['merge_key'] = details_df.apply(lambda r: f"{r.get('url','')}|{r.get('item_name','')}", axis=1)
        metrics_df['merge_key'] = metrics_df.apply(lambda r: f"{r.get('page','')}|{r.get('item_name','')}", axis=1)
        details_df = details_df.merge(metrics_df.drop(columns=['page']), on='merge_key', how='left', suffixes=('','_metric'))
        details_df.drop(columns=['merge_key'], inplace=True)

    for col in ['url','item_name']:
        if col not in details_df.columns:
            details_df[col] = None

    return details_df, metrics_df

# =================================================
# BLOCK 6: Unique key
# =================================================
def build_unique_key_series(df, site, enhancement_name, date_val, status_col='status'):
    def row_key(r):
        page = r.get('url') or ""
        item_name = r.get('item_name') or ""
        status = r.get(status_col) or ""
        lastc = r.get('last_crawled')
        if isinstance(lastc, datetime):
            lastc = lastc.date()
        lastc_str = str(lastc) if lastc else ""
        date_str = str(date_val) if date_val else ""
        raw = f"{site}|{enhancement_name}|{page}|{item_name}|{status}|{lastc_str}|{date_str}"
        return hashlib.sha256(raw.encode('utf-8')).hexdigest()
    return df.apply(row_key, axis=1)

# =================================================
# BLOCK 7: BQ helpers
# =================================================
def ensure_table_exists():
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        table = bq_client.get_table(table_ref)
        print(f"[INFO] Table exists.")
    except Exception:
        print(f"[INFO] Creating table {TABLE_ID}...")
        schema = [bigquery.SchemaField(c, "STRING") for c in FINAL_COLUMNS]
        date_cols = ["date","last_crawled","fetch_date"]
        for c in date_cols:
            if c not in [f.name for f in schema]:
                schema.append(bigquery.SchemaField(c,"DATE"))
        table = bigquery.Table(table_ref, schema=schema)
        bq_client.create_table(table)
        print(f"[INFO] Table created.")

def get_existing_unique_keys():
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"
    try:
        df = bq_client.query(f"SELECT unique_key FROM `{table_ref}`").to_dataframe()
        return set(df['unique_key'].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Could not fetch existing keys: {e}")
        return set()

def get_mapping_dict():
    try:
        df = bq_client.query(f"SELECT SearchAppearance, Enhancement_Name FROM `{MAPPING_TABLE}`").to_dataframe()
        df['enh_norm'] = df['Enhancement_Name'].astype(str).str.strip().str.lower()
        return dict(zip(df['enh_norm'], df['SearchAppearance']))
    except Exception:
        return {}

# =================================================
# BLOCK 8: Upload helper
# =================================================
def upload_to_bq(df):
    if df is None or df.empty:
        print("[INFO] No new rows to upload.")
        return
    table_ref = bq_client.dataset(DATASET_ID).table(TABLE_ID)
    try:
        table = bq_client.get_table(table_ref)
        allowed_cols = [f.name for f in table.schema]
    except Exception:
        allowed_cols = FINAL_COLUMNS

    df = df.copy()
    for c in allowed_cols:
        if c not in df.columns:
            df[c] = None
    df = df[allowed_cols]

    for dcol in ['date','last_crawled','fetch_date']:
        df[dcol] = pd.to_datetime(df[dcol], errors='coerce').dt.strftime("%Y-%m-%d")
    df = df.where(pd.notnull(df), None)

    if DEBUG_MODE:
        out_fn = f"gsc_enhancements_upload_preview_{FETCH_ID}.csv"
        df.to_csv(out_fn, index=False)
        print(f"[DEBUG] Wrote preview CSV: {out_fn} (rows: {len(df)})")
        return

    try:
        job_config = bigquery.LoadJobConfig(write_disposition="WRITE_APPEND")
        job = bq_client.load_table_from_dataframe(df, f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}", job_config=job_config)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to {PROJECT_ID}.{DATASET_ID}.{TABLE_ID}.")
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}")

# =================================================
# BLOCK 9: MAIN
# =================================================
def main():
    ensure_table_exists()
    existing_keys = get_existing_unique_keys()
    mapping_dict = get_mapping_dict()
    all_new = []

    if not os.path.isdir(enhancement_folder):
        print(f"[ERROR] Local path '{enhancement_folder}' not found.")
        return

    for fname in sorted(os.listdir(enhancement_folder)):
        if not fname.lower().endswith(".xlsx"):
            continue
        file_path = os.path.join(enhancement_folder, fname)
        meta = parse_filename_metadata(fname)
        site = meta.get("site") or ""
        enhancement_name = meta.get("enhancement_name") or ""
        date_val = meta.get("date")
        status_hint = meta.get("status_hint")
        source_file = meta.get("source_file")

        print(f"[INFO] Processing file: {fname} -> enhancement='{enhancement_name}', date={date_val}, status_hint={status_hint}")

        details_df, metrics_df = parse_excel_file(file_path)
        if details_df.empty and metrics_df.empty:
            print(f"[INFO] No data found — skipping.")
            continue

        if not details_df.empty:
            details_df = details_df[details_df['url'].notna()]
            details_df['site'] = site
            details_df['date'] = date_val
            details_df['enhancement_name'] = enhancement_name
            if 'status' in details_df.columns:
                details_df['status'] = details_df['status'].fillna(status_hint or "Unknown")
            else:
                details_df['status'] = status_hint or "Unknown"
            details_df['fetch_id'] = FETCH_ID
            details_df['fetch_date'] = FETCH_DATE
            details_df['source_file'] = source_file
            details_df['last_crawled'] = details_df.get('last_crawled')

            # Map enhancement_name -> appearance_type
            norm_name = enhancement_name.strip().lower()
            details_df['appearance_type'] = mapping_dict.get(norm_name, None)

            # Unique key
            details_df['unique_key'] = build_unique_key_series(details_df, site, enhancement_name, date_val)
            # Remove duplicates within batch
            before_count = len(details_df)
            details_df = details_df.drop_duplicates(subset=['unique_key']).reset_index(drop=True)
            after_count = len(details_df)
            if before_count != after_count:
                print(f"[INFO] Removed {before_count-after_count} duplicate rows in file {fname}")
        else:
            continue

        all_new.append(details_df)

    final_df = pd.concat(all_new, ignore_index=True) if all_new else pd.DataFrame()
    if not final_df.empty:
        # Remove duplicates across all new rows
        final_df = final_df.drop_duplicates(subset=['unique_key']).reset_index(drop=True)
        print(f"[INFO] Total new rows to upload: {len(final_df)}")
    upload_to_bq(final_df)

if __name__ == "__main__":
    main()
