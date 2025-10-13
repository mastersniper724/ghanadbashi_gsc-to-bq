import pandas as pd
from google.cloud import bigquery

# Client
client = bigquery.Client()

# Query dataset کامل (مثال جدول weekly summary)
query = """
SELECT *
FROM `ghanadbashi.seo_reports.00_02__ghanadbashi__gsc__raw_domain_data_webtype_fullfetch_null_safe_cast`
"""

# Job configuration برای export به Parquet
job_config = bigquery.QueryJobConfig()
destination_uri = "gsc_full_dataset.parquet"

# Run query and fetch all rows
query_job = client.query(query)
rows = query_job.result()  # this fetches all rows (pagination handled internally)

# Write to Parquet using pandas
import pandas as pd

df = rows.to_dataframe()
df.to_parquet(destination_uri, engine="pyarrow", index=False)
print(f"Parquet created: {destination_uri}, rows: {len(df)}")
