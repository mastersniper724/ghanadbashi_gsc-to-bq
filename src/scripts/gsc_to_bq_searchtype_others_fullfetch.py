#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_to_bq_othersearchtypes_fullfetch.py
# Revision: Rev.0
# Purpose: Full fetch from GSC -> for Image / Video / News Search Types
# Notes: Fixes for pagination loop, ensures SearchType=image is processed,
#        and updates BigQuery table schema to include SearchType if missing.
# ============================================================

import os
import sys
import time
import hashlib
import argparse
import warnings
import pandas as pd
from datetime import datetime
from google.oauth2 import service_account
from googleapiclient.discovery import build
from google.cloud import bigquery
from utils.gsc_country_utils import load_country_map, robust_map_country_column

# ---------- MAIN CONFIG ----------
SITE_URL = "sc-domain:ghanadbashi.com"
BQ_PROJECT = "ghanadbashi"
BQ_DATASET = "seo_reports"
BQ_TABLE = "00_03__temp_ghanadbashi__gsc__raw_domain_data_othersearchtype_fullfetch"
ROW_LIMIT = 25000
RETRY_DELAY = 60  # seconds
SERVICE_ACCOUNT_FILE = os.environ.get("SERVICE_ACCOUNT_FILE", "gcp-key.json")
# ensure image is present first to avoid accidental omission
SEARCH_TYPES = ['image', 'video', 'news']

# ---------- ARGUMENTS ----------
parser = argparse.ArgumentParser(description="GSC to BigQuery Full Fetch (Rev5)")
parser.add_argument("--start-date", required=True, help="Start date YYYY-MM-DD")
parser.add_argument("--end-date", required=True, help="End date YYYY-MM-DD")
parser.add_argument("--debug", action="store_true", help="Debug: skip BQ insert (still creates CSV if requested)")
parser.add_argument("--csv-test", required=False, help="Optional CSV test output filename")
args = parser.parse_args()

START_DATE = args.start_date
END_DATE = args.end_date
DEBUG_MODE = args.debug
CSV_TEST_FILE = args.csv_test

# ---------- CLIENTS ----------
def get_credentials():
    creds = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    return creds

def get_bq_client():
    creds_for_bq = service_account.Credentials.from_service_account_file(SERVICE_ACCOUNT_FILE)
    return bigquery.Client(credentials=creds_for_bq, project=creds_for_bq.project_id)

def get_gsc_service():
    creds_for_gsc = service_account.Credentials.from_service_account_file(
        SERVICE_ACCOUNT_FILE,
        scopes=["https://www.googleapis.com/auth/webmasters.readonly"]
    )
    return build("searchconsole", "v1", credentials=creds_for_gsc)

bq_client = get_bq_client()
table_ref = bq_client.dataset(BQ_DATASET).table(BQ_TABLE)

# ---------- COUNTRY MAPPING ----------
client = bigquery.Client()
query = """
    SELECT country_code_alpha3 AS country_code, country_name
    FROM `ghanadbashi.seo_reports.00_00_gsc_dim_country`
"""
df_country = client.query(query).to_dataframe()
df_country["country_code"] = df_country["country_code"].str.upper()
COUNTRY_MAP = dict(zip(df_country["country_code"], df_country["country_name"]))

# ---------- ENSURE TABLE EXISTS & SCHEMA UPDATES ----------
def ensure_table_and_schema():
    """
    Ensure table exists. If exists but missing 'SearchType' column, add it.
    This avoids the 'Provided Schema does not match Table ... Cannot add fields (field: SearchType)' error.
    """
    try:
        table = bq_client.get_table(table_ref)
        print(f"[INFO] Table {BQ_TABLE} exists.", flush=True)
        # check schema for SearchType
        field_names = [f.name for f in table.schema]
        if "SearchType" not in field_names:
            print(f"[INFO] Field 'SearchType' missing in table schema. Attempting to add it...", flush=True)
            new_schema = list(table.schema)
            new_schema.append(bigquery.SchemaField("SearchType", "STRING"))
            table.schema = new_schema
            try:
                bq_client.update_table(table, ["schema"])
                print(f"[INFO] Added 'SearchType' column to {BQ_TABLE}.", flush=True)
            except Exception as e:
                # fallback: try ALTER TABLE via query (may require permissions)
                try:
                    alter_q = f"ALTER TABLE `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` ADD COLUMN SearchType STRING"
                    query_job = bq_client.query(alter_q)
                    query_job.result()
                    print(f"[INFO] Added 'SearchType' column via ALTER TABLE.", flush=True)
                except Exception as e2:
                    print(f"[WARN] Could not add 'SearchType' column automatically: {e} / {e2}", flush=True)
                    print("[WARN] Please add column 'SearchType' (STRING) manually to the table and re-run.", flush=True)
    except Exception:
        print(f"[INFO] Table {BQ_TABLE} not found. Creating...", flush=True)
        schema = [
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("SearchType", "STRING"),
            bigquery.SchemaField("unique_key", "STRING"),
        ]
        table = bigquery.Table(table_ref, schema=schema)
        table.clustering_fields = ["Page", "Query", "Country", "Device"]
        bq_client.create_table(table)
        print(f"[INFO] Table {BQ_TABLE} created with SearchType field.", flush=True)

# ---------- UNIQUE KEY ----------
def generate_unique_key(row):
    q = (row.get("Query") or "").strip().lower()
    p = (row.get("Page") or "").strip().lower().rstrip("/")
    c = (row.get("Country") or "").strip().lower()
    d = (row.get("Device") or "").strip().lower()
    date_raw = row.get("Date")
    if isinstance(date_raw, str):
        date = date_raw[:10]
    elif isinstance(date_raw, datetime):
        date = date_raw.strftime("%Y-%m-%d")
    else:
        date = str(date_raw)[:10]
    key_str = "|".join([date, q, p, c, d])
    return hashlib.sha256(key_str.encode("utf-8")).hexdigest()

# ---------- GET EXISTING KEYS (LIMITED BY DATE RANGE) ----------
def get_existing_keys(start_date, end_date):
    try:
        query = (
            f"SELECT unique_key FROM `{BQ_PROJECT}.{BQ_DATASET}.{BQ_TABLE}` "
            f"WHERE Date BETWEEN '{start_date}' AND '{end_date}'"
        )
        try:
            from google.cloud import bigquery_storage
            bqstorage_client = bigquery_storage.BigQueryReadClient()
            df = bq_client.query(query).to_dataframe(bqstorage_client=bqstorage_client)
        except Exception:
            with warnings.catch_warnings():
                warnings.simplefilter("ignore")
                df = bq_client.query(query).to_dataframe()
        print(f"[INFO] Retrieved {len(df)} existing keys from BigQuery (date-filtered).", flush=True)
        return set(df["unique_key"].astype(str).tolist())
    except Exception as e:
        print(f"[WARN] Failed to fetch existing keys: {e}", flush=True)
        return set()

# ---------- UPLOAD TO BIGQUERY ----------
def upload_to_bq(df):
    if df.empty:
        print("[INFO] No new rows to insert.", flush=True)
        return 0
    df["Date"] = pd.to_datetime(df["Date"])
    if DEBUG_MODE:
        print(f"[DEBUG] Debug mode ON: skipping insert of {len(df)} rows to BigQuery", flush=True)
        return len(df)
    job_config = bigquery.LoadJobConfig(
        write_disposition="WRITE_APPEND",
        schema=[
            bigquery.SchemaField("Date", "DATE"),
            bigquery.SchemaField("Query", "STRING"),
            bigquery.SchemaField("Page", "STRING"),
            bigquery.SchemaField("Country", "STRING"),
            bigquery.SchemaField("Device", "STRING"),
            bigquery.SchemaField("SearchAppearance", "STRING"),
            bigquery.SchemaField("Clicks", "INTEGER"),
            bigquery.SchemaField("Impressions", "INTEGER"),
            bigquery.SchemaField("CTR", "FLOAT"),
            bigquery.SchemaField("Position", "FLOAT"),
            bigquery.SchemaField("SearchType", "STRING"),
            bigquery.SchemaField("unique_key", "STRING"),
        ],
    )
    try:
        job = bq_client.load_table_from_dataframe(df, table_ref, job_config=job_config)
        job.result()
        print(f"[INFO] Inserted {len(df)} rows to BigQuery.", flush=True)
        return len(df)
    except Exception as e:
        print(f"[ERROR] Failed to insert rows: {e}", flush=True)
        return 0

# ---------- FETCH GSC DATA ----------
def fetch_gsc_data(start_date, end_date, existing_keys):
    """
    Main batches (keeps original DIMENSION_BATCHES).
    Pagination is handled per (dims, searchType) with start_row reset for each pair.
    Returns (df_all_new, total_inserted)
    """
    service = get_gsc_service()
    all_new_rows = []
    total_inserted = 0

    DIMENSION_BATCHES = [
        ["date", "query", "page"],
        ["date", "query", "country"],
        ["date", "query", "device"],
        ["date", "query"],
    ]

    total_fetched_overall = 0
    total_new_candidates_overall = 0

    for i, dims in enumerate(DIMENSION_BATCHES, start=1):
        fetched_total_for_batch = 0
        new_candidates_for_batch = 0

        for stype in SEARCH_TYPES:
            # reset pagination per (dims, stype)
            start_row = 0
            page_index = 0
            print(f"[INFO] Batch {i}, dims={dims}, starting fetch for SearchType={stype}", flush=True)

            while True:
                request = {
                    "startDate": start_date,
                    "endDate": end_date,
                    "dimensions": dims,
                    "rowLimit": ROW_LIMIT,
                    "startRow": start_row,
                    "searchType": stype,
                }

                try:
                    resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
                except Exception as e:
                    print(f"[ERROR] Timeout or GSC error for dims={dims}, stype={stype}: {e}. Retrying in {RETRY_DELAY}s...", flush=True)
                    time.sleep(RETRY_DELAY)
                    continue

                rows = resp.get("rows", [])
                if not rows:
                    print(f"[INFO] No rows returned for dims={dims}, stype={stype}, startRow={start_row}", flush=True)
                    break  # no more rows for this (dims,stype)

                fetched_count = len(rows)
                fetched_total_for_batch += fetched_count
                page_index += 1

                batch_new = []
                for r in rows:
                    keys = r.get("keys", [])
                    date = keys[0] if len(keys) > 0 else None
                    query = keys[1] if ("query" in dims and len(keys) > 1) else None
                    third = keys[2] if len(keys) > 2 else None
                    page = third if "page" in dims else None
                    country = third if "country" in dims else None
                    device = third if "device" in dims else None

                    row = {
                        "Date": date,
                        "Query": query,
                        "Page": page,
                        "Country": country,
                        "Device": device,
                        "SearchAppearance": "__NO_APPEARANCE__",  #Null
                        "Clicks": r.get("clicks", 0),
                        "Impressions": r.get("impressions", 0),
                        "CTR": r.get("ctr", 0.0),
                        "Position": r.get("position", 0.0),
                        "SearchType": stype,
                    }

                    unique_key = generate_unique_key(row)
                    if unique_key not in existing_keys:
                        existing_keys.add(unique_key)
                        row["unique_key"] = unique_key
                        batch_new.append(row)

                new_candidates_for_batch += len(batch_new)
                if batch_new:
                    df_batch = pd.DataFrame(batch_new)

                    # ---------- APPLY COUNTRY MAPPING FOR THIS BATCH (if applicable) ----------
                    # only attempt mapping for batches that requested the 'country' dimension
                    if "country" in [d.lower() for d in dims]:
                        # find actual country column name in df_batch (case-insensitive)
                        country_col = next((c for c in df_batch.columns if c.lower() == "country"), None)

                        if country_col is None:
                            print(f"[DEBUG] Batch {i}: expected 'country' column but none found in columns. Skipping country mapping.", flush=True)
                        else:
                            # quick samples to inspect incoming codes
                            sample_vals = pd.Series(df_batch[country_col].astype(str)).dropna().unique()[:20]

                            # apply robust mapping (uses utils.robust_map_country_column)
                            df_batch = robust_map_country_column(df_batch, country_col=country_col, country_map=COUNTRY_MAP, new_col="Country")
                            # now show how many mapped / unmapped
                            mapped_count = df_batch["Country"].notna().sum()
                            total_count = len(df_batch)

                    # ---------- UPLOAD to BQ ----------
                    inserted = upload_to_bq(df_batch)
                    total_inserted += inserted
                    all_new_rows.extend(batch_new)

                # pagination control:
                # if rows < ROW_LIMIT -> no further pages for this (dims, stype)
                if fetched_count < ROW_LIMIT:
                    print(f"[INFO] End of pages for dims={dims}, stype={stype} (fetched_count={fetched_count} < ROW_LIMIT)", flush=True)
                    break
                # else increment start_row and continue
                start_row += fetched_count
                # safety guard to avoid runaway loops: break if too many pages (e.g., >1000)
                if page_index > 10000:
                    print(f"[WARN] Too many pages for dims={dims}, stype={stype}. Breaking to avoid infinite loop.", flush=True)
                    break

        print(f"[INFO] Batch {i} summary: fetched_total={fetched_total_for_batch}, new_candidates={new_candidates_for_batch}", flush=True)
        total_fetched_overall += fetched_total_for_batch
        total_new_candidates_overall += new_candidates_for_batch

    df_all_new = pd.DataFrame(all_new_rows)
    print(f"[INFO] Fetch_GSC_Data summary: fetched_overall={total_fetched_overall}, new_candidates_overall={total_new_candidates_overall}, inserted_overall={total_inserted}", flush=True)
    return df_all_new, total_inserted

# ---------- Batch 5: Isolated No-Index fetch ----------
def fetch_noindex_batch(start_date, end_date, existing_keys):
    """
    Fetch rows where 'page' is NULL/empty in dimensions ['date','page'].
    Returns (df_noindex, inserted_count)
    """
    service = get_gsc_service()
    noindex_rows = []
    inserted_total = 0
    fetched_total = 0
    new_candidates = 0

    for stype in SEARCH_TYPES:
        start_row = 0
        while True:
            print(f"[INFO] Batch 5, No-Index fetch for stype={stype}, startRow={start_row}", flush=True)
            request = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": ["date", "page"],
                "rowLimit": ROW_LIMIT,
                "startRow": start_row,
                "searchType": stype,
            }
            try:
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            except Exception as e:
                print(f"[ERROR] Batch 5, No-Index fetch error for stype={stype}: {e}, retrying in {RETRY_DELAY} sec...", flush=True)
                time.sleep(RETRY_DELAY)
                continue

            rows = resp.get("rows", [])
            if not rows:
                break

            fetched_total += len(rows)
            for r in rows:
                keys = r.get("keys", [])
                # expect [date, page]
                if len(keys) == 2:
                    page_val = keys[1]
                    if (page_val is None) or (str(page_val).strip() == ""):
                        row = {
                            "Date": keys[0],
                            "Query": "__NO_INDEX__",
                            "Page": "__NO_INDEX__",
                            "Country": "__NO_COUNTRY__",
                            "Device": "__NO_DEVICE__",
                            "SearchAppearance": "__NO_APPEARANCE__",
                            "Clicks": r.get("clicks", 0),
                            "Impressions": r.get("impressions", 0),
                            "CTR": r.get("ctr", 0.0),
                            "Position": r.get("position", 0.0),
                            "SearchType": stype,
                        }
                        row["unique_key"] = generate_unique_key(row)
                        if row["unique_key"] not in existing_keys:
                            existing_keys.add(row["unique_key"])
                            noindex_rows.append(row)
                            new_candidates += 1

            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

    df_noindex = pd.DataFrame(noindex_rows) if noindex_rows else pd.DataFrame([])
    if not df_noindex.empty:
        inserted = upload_to_bq(df_noindex)
        inserted_total += inserted
        print(f"[INFO] Batch 5, No-Index: fetched={fetched_total}, new_candidates={new_candidates}, inserted={inserted}", flush=True)
    else:
        print(f"[INFO] Batch 5, No-Index: no new rows found.", flush=True)

    return df_noindex, inserted_total

# ---------- Batch 7: SITEWIDE (ISOLATED) ----------
def fetch_sitewide_batch(start_date, end_date, existing_keys):
    """
    Sitewide: dimensions = ['date']
    Inserts __SITE_TOTAL__ rows per searchType and placeholder dates for missing days.
    Returns (df_all_new_rows, inserted_count)
    """
    print("[INFO] Batch 7: Running sitewide ['date']...", flush=True)
    service = get_gsc_service()
    all_new_rows = []
    total_new_count = 0
    fetched_total = 0
    new_candidates = 0

    for stype in SEARCH_TYPES:
        start_row = 0
        while True:
            print(f"[INFO] Batch 7: Sitewide fetch for stype={stype}, startRow={start_row}", flush=True)
            request = {
                "startDate": start_date,
                "endDate": end_date,
                "dimensions": ["date"],
                "rowLimit": ROW_LIMIT,
                "startRow": start_row,
                "searchType": stype,
            }
            try:
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
            except Exception as e:
                print(f"[ERROR] Batch 7: Sitewide error (stype={stype}): {e}, retrying in {RETRY_DELAY} sec...", flush=True)
                time.sleep(RETRY_DELAY)
                continue

            rows = resp.get("rows", [])
            if not rows:
                break

            fetched_total += len(rows)
            batch_new = []
            for r in rows:
                keys = r.get("keys", [])
                date = keys[0] if len(keys) > 0 else None

                row = {
                    "Date": date,
                    "Query": "__SITE_TOTAL__",
                    "Page": "__SITE_TOTAL__",
                    "Country": "__NO_COUNTRY__",
                    "Device": "__NO_DEVICE__",
                    "SearchAppearance": "__NO_APPEARANCE__",
                    "Clicks": r.get("clicks", 0),
                    "Impressions": r.get("impressions", 0),
                    "CTR": r.get("ctr", 0.0),
                    "Position": r.get("position", 0.0),
                    "SearchType": stype,
                }

                unique_key = generate_unique_key(row)
                if unique_key not in existing_keys:
                    existing_keys.add(unique_key)
                    row["unique_key"] = unique_key
                    batch_new.append(row)
                    new_candidates += 1

            if batch_new:
                df_batch = pd.DataFrame(batch_new)
                inserted = upload_to_bq(df_batch)
                total_new_count += inserted
                all_new_rows.extend(batch_new)

            if len(rows) < ROW_LIMIT:
                break
            start_row += len(rows)

    # Step 2: placeholders for missing dates (for each searchType)
    date_range = pd.date_range(start=start_date, end=end_date)
    for dt in date_range:
        date_str = dt.strftime("%Y-%m-%d")
        # check if any row for that date exists in all_new_rows for any stype
        exists_for_date = any(str(row.get("Date"))[:10] == date_str for row in all_new_rows)
        if not exists_for_date:
            for stype in SEARCH_TYPES:
                placeholder_row = {
                    "Date": date_str,
                    "Query": "__SITE_TOTAL__",
                    "Page": "__SITE_TOTAL__",
                    "Country": "__NO_COUNTRY__",
                    "Device": "__NO_DEVICE__",
                    "SearchAppearance": "__NO_APPEARANCE__",  #Null
                    "Clicks": 0,
                    "Impressions": 0,
                    "CTR": 0,
                    "Position": 0,
                    "SearchType": stype,
                }
                unique_key = generate_unique_key(placeholder_row)
                if unique_key not in existing_keys:
                    existing_keys.add(unique_key)
                    placeholder_row["unique_key"] = unique_key
                    all_new_rows.append(placeholder_row)
                    print(f"[INFO] Batch 7, Sitewide: adding placeholder for missing date {date_str} (stype={stype})", flush=True)

    placeholders_only = [row for row in all_new_rows if row.get("Clicks") is None]
    if placeholders_only:
        df_placeholders = pd.DataFrame(placeholders_only)
        inserted = upload_to_bq(df_placeholders)
        total_new_count += inserted

    print(f"[INFO] Batch 7, Sitewide done: fetched_total={fetched_total}, new_candidates={new_candidates}, inserted={total_new_count}", flush=True)
    return pd.DataFrame(all_new_rows), total_new_count

# ---------- MAIN ----------
def main():
    ensure_table_and_schema()
    print(f"[INFO] Fetching data from {START_DATE} to {END_DATE}", flush=True)

    # ---------- Check existing keys (once, date-limited) ----------
    existing_keys = get_existing_keys(START_DATE, END_DATE)
    print(f"[INFO] Retrieved {len(existing_keys)} existing keys from BigQuery. (used across all blocks)", flush=True)

    # --- Normal FullFetch Batch (main pipeline) ---
    df_new, inserted_main = fetch_gsc_data(START_DATE, END_DATE, existing_keys)

    # --- Isolated No-Index pass (replaces Batch7) ---
    df_noindex, inserted_noindex = fetch_noindex_batch(START_DATE, END_DATE, existing_keys)

    # ----------------------------
    # B. Fetch Batch 4: Date + Page (Page IS NOT NULL)
    # ----------------------------
    print("[INFO] Fetching Batch 6 (Date + Page, excluding NULL pages)...", flush=True)
    try:
        service = get_gsc_service()
        all_rows = []
        fetched_b4 = 0
        new_b4 = 0

        for stype in SEARCH_TYPES:
            start_row = 0
            while True:
                print(f"[INFO] Batch 6 fetch for stype={stype}, startRow={start_row}", flush=True)
                request = {
                    "startDate": START_DATE,
                    "endDate": END_DATE,
                    "dimensions": ["date", "page"],
                    "rowLimit": ROW_LIMIT,
                    "startRow": start_row,
                    "searchType": stype,
                }
                resp = service.searchanalytics().query(siteUrl=SITE_URL, body=request).execute()
                rows = resp.get("rows", [])
                if not rows:
                    break

                fetched_b4 += len(rows)
                for r in rows:
                    keys = r.get("keys", [])
                    if len(keys) == 2 and keys[1]:  # only non-null pages
                        row = {
                            "Date": keys[0],
                            "Query": "__PAGE_TOTAL__",
                            "Page": keys[1],
                            "Country": "__NO_COUNTRY__",
                            "Device": "__NO_DEVICE__",
                            "SearchAppearance": "__NO_APPEARANCE__",
                            "Clicks": r.get("clicks", 0),
                            "Impressions": r.get("impressions", 0),
                            "CTR": r.get("ctr", 0.0),
                            "Position": r.get("position", 0.0),
                            "SearchType": stype,
                        }
                        unique_key = generate_unique_key(row)
                        if unique_key not in existing_keys:
                            existing_keys.add(unique_key)
                            row["unique_key"] = unique_key
                            all_rows.append(row)
                            new_b4 += 1

                if len(rows) < ROW_LIMIT:
                    break
                start_row += len(rows)

        inserted_b4 = 0
        if all_rows:
            df_batch4 = pd.DataFrame(all_rows)
            print(f"[INFO] Batch 6 fetched rows: {len(df_batch4)}", flush=True)
            if not df_batch4.empty:
                inserted_b4 = upload_to_bq(df_batch4)
                print(f"[INFO] Batch 6: Inserted {inserted_b4} new rows to BigQuery.", flush=True)
        else:
            df_batch4 = pd.DataFrame([])
            print("[INFO] Batch 6: No non-null page rows found.", flush=True)

        print(f"[INFO] Batch 6 summary: fetched_total={fetched_b4}, new_candidates={new_b4}, inserted={inserted_b4}", flush=True)

    except Exception as e:
        print(f"[ERROR] Failed to fetch Batch 6 (Date + Page): {e}", flush=True)
        inserted_b4 = 0
        df_batch4 = pd.DataFrame([])

    # --- run isolated sitewide batch ---
    df_site, inserted_site = fetch_sitewide_batch(START_DATE, END_DATE, existing_keys)

    total_all_inserted = inserted_main + inserted_noindex + inserted_b4 + inserted_site

    # Compose CSV output if requested
    if CSV_TEST_FILE:
        try:
            parts = []
            if 'df_new' in locals() and not df_new.empty:
                parts.append(df_new)
            if 'df_noindex' in locals() and not df_noindex.empty:
                parts.append(df_noindex)
            if 'df_batch4' in locals() and not df_batch4.empty:
                parts.append(df_batch4)
            if 'df_site' in locals() and not df_site.empty:
                parts.append(df_site)
            if parts:
                df_combined = pd.concat(parts, ignore_index=True)
                df_combined.to_csv(CSV_TEST_FILE, index=False)
                print(f"[INFO] CSV test output written: {CSV_TEST_FILE}", flush=True)
            else:
                # write empty csv with headers
                cols = ["Date","Query","Page","Country","Device","SearchAppearance","Clicks","Impressions","CTR","Position","SearchType","unique_key"]
                pd.DataFrame(columns=cols).to_csv(CSV_TEST_FILE, index=False)
                print(f"[INFO] CSV test output written (empty): {CSV_TEST_FILE}", flush=True)
        except Exception as e:
            print(f"[WARN] Failed to write CSV test file: {e}", flush=True)

    # Final summary
    print("[INFO] Final summary:", flush=True)
    print(f"  - fetch_gsc_data inserted: {inserted_main}", flush=True)
    print(f"  - noindex inserted:       {inserted_noindex}", flush=True)
    print(f"  - batch4 inserted:        {inserted_b4}", flush=True)
    print(f"  - sitewide inserted:      {inserted_site}", flush=True)
    print(f"[INFO] Total new rows fetched/inserted: {total_all_inserted}", flush=True)
    print("[INFO] Finished.", flush=True)


if __name__ == "__main__":
    main()
