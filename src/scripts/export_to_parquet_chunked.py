# =====================================================
# File: export_to_parquet_chunked.py
# Purpose: Export large GSC dataset from BigQuery to a single Parquet file in chunks
# =====================================================

from google.cloud import bigquery
import pandas as pd
import pyarrow.parquet as pq
import pyarrow as pa

# ===================== CONFIG ========================
PROJECT_ID = "ghanadbashi"   # جایگذاری با پروژه شما
DATASET = "seo_reports"     # dataset شما
TABLE = "ghanadbashi.seo_reports.00_02__ghanadbashi__gsc__raw_domain_data_webtype_fullfetch_null_safe_cast"         # جدول هدف
OUTPUT_FILE = "/home/master_sniper724/gsc_weekly_summary_full.parquet"
CHUNK_SIZE = 10000           # تعداد ردیف در هر batch

# ===================== CLIENT ========================
client = bigquery.Client(project=PROJECT_ID)

# ===================== QUERY =========================
query = f"""
SELECT *
FROM `{PROJECT_ID}.{DATASET}.{TABLE}`
ORDER BY Date, Query
"""
query_job = client.query(query)

# ===================== CHUNKED EXPORT =================
dfs = []
print(f"Start reading data in chunks of {CHUNK_SIZE} rows...")
iterator = query_job.result(page_size=CHUNK_SIZE)

for i, page in enumerate(iterator.pages, start=1):
    df_chunk = page.to_dataframe()
    dfs.append(df_chunk)
    print(f"Chunk {i} read, {len(df_chunk)} rows.")

# ===================== CONCAT & EXPORT =================
full_df = pd.concat(dfs, ignore_index=True)
print(f"Total rows collected: {len(full_df)}")
table = pa.Table.from_pandas(full_df)
pq.write_table(table, OUTPUT_FILE, compression="SNAPPY")
print(f"Data exported successfully to {OUTPUT_FILE}")
