from google.cloud import bigquery
client = bigquery.Client()
list(client.list_datasets())
