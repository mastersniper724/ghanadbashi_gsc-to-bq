from google.cloud import bigquery
import pandas as pd

# اتصال به BigQuery
client = bigquery.Client(project="ghanadbashi")

# کوئری نمونه (میتونی با هر جدولی جایگزینش کنی)
query = """
SELECT * 
FROM `ghanadbashi.seo_reports.00_02__ghanadbashi__gsc__raw_domain_data_webtype_fullfetch_null_safe_cast`
LIMIT 5000
"""

# اجرای کوئری و گرفتن دیتا در DataFrame
df = client.query(query).to_dataframe()

# ذخیره در فایل Parquet داخل Cloud Shell
output_path = "/home/master_sniper724/gsc_weekly_summary.parquet"
df.to_parquet(output_path, index=False)
print(f"✅ File saved at: {output_path}")


