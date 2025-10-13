#!/usr/bin/env python3
# -*- coding: utf-8 -*-

import os
import pandas as pd
from google.cloud import bigquery

# ======================
# تنظیمات
# ======================
PROJECT_ID = "ghanadbashi"  # Project ID درست
DATASET_TABLE = "seo_reports.00_02__ghanadbashi__gsc__raw_domain_data_webtype_fullfetch_null_safe_cast"  # Dataset.Table درست
CHUNK_SIZE = 10000          # تعداد ردیف هر چانک
OUTPUT_DIR = "./"           # مسیر ذخیره فایل Parquet

# ======================
# اتصال به BigQuery
# ======================
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = "/home/master_sniper724/gcp-key.json"
client = bigquery.Client(project=PROJECT_ID)

# ======================
# اجرای کوئری
# ======================
query = f"SELECT * FROM `{PROJECT_ID}.{DATASET_TABLE}`"
query_job = client.query(query)
iterator = query_job.result()  # بدون page_size → RowIterator

# ======================
# خواندن چانک به چانک
# ======================
print(f"Start reading data in chunks of {CHUNK_SIZE} rows...")

chunk_number = 0
current_chunk = []

for i, row in enumerate(iterator):
    current_chunk.append(dict(row))
    
    if (i + 1) % CHUNK_SIZE == 0:
        df_chunk = pd.DataFrame(current_chunk)
        output_file = os.path.join(OUTPUT_DIR, f"gsc_weekly_summary_chunk_{chunk_number}.parquet")
        df_chunk.to_parquet(output_file, index=False)
        print(f"Saved chunk {chunk_number} → {output_file} ({len(df_chunk)} rows)")
        current_chunk = []
        chunk_number += 1

# ذخیره باقی‌مانده ردیف‌ها
if current_chunk:
    df_chunk = pd.DataFrame(current_chunk)
    output_file = os.path.join(OUTPUT_DIR, f"gsc_weekly_summary_chunk_{chunk_number}.parquet")
    df_chunk.to_parquet(output_file, index=False)
    print(f"Saved final chunk {chunk_number} → {output_file} ({len(df_chunk)} rows)")

print("All chunks saved successfully!")
