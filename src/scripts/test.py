from google.cloud import bigquery
import pandas as pd

client = bigquery.Client()
query = "SELECT * FROM `ghanadbashi.seo_reports.gsc_full_data` LIMIT 1000"
df = client.query(query).to_dataframe()
df.to_parquet("gsc_export.parquet", index=False)
