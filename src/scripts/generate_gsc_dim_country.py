# ============================================================
# File: generate_gsc_dim_country.py
# Version: 0
# Purpose: Generate a full ISO 3166-1 country dimension table
#          with both Alpha-2 and Alpha-3 codes for BigQuery.
# Author: MasterSniper ETL Module
# ============================================================
import pandas as pd
import pycountry
from google.cloud import bigquery

# =================================================
# BLOCK 1: CONFIGURATION & ARGUMENT PARSING
# =================================================
PROJECT_ID = "ghanadbashi"
DATASET_ID = "seo_reports"
TABLE_ID = "00_00_gsc_dim_country"

# ---------- BUILD COUNTRY DATA ----------
def build_country_dataframe() -> pd.DataFrame:
    """
    Creates a DataFrame of countries with:
      - country_name
      - country_code_alpha2
      - country_code_alpha3
    based on ISO 3166-1 data from pycountry.
    """
    records = []
    for country in pycountry.countries:
        records.append({
            "country_name": country.name,
            "country_code_alpha2": country.alpha_2.upper(),
            "country_code_alpha3": country.alpha_3.upper(),
        })

    df = pd.DataFrame(records)
    df = df.sort_values(by="country_name").reset_index(drop=True)
    return df


# ---------- WRITE TO BIGQUERY ----------
def write_to_bigquery(df: pd.DataFrame):
    """
    Writes the DataFrame to BigQuery, replacing the existing table.
    """
    client = bigquery.Client(project=PROJECT_ID)
    table_ref = f"{PROJECT_ID}.{DATASET_ID}.{TABLE_ID}"

    job_config = bigquery.LoadJobConfig(
        write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE,
        autodetect=False,
        schema=[
            bigquery.SchemaField("country_name", "STRING"),
            bigquery.SchemaField("country_code_alpha2", "STRING"),
            bigquery.SchemaField("country_code_alpha3", "STRING"),
        ],
    )

    load_job = client.load_table_from_dataframe(df, table_ref, job_config=job_config)
    load_job.result()  # Wait for job to complete


# ---------- MAIN ----------
if __name__ == "__main__":
    df_country = build_country_dataframe()
    write_to_bigquery(df_country)
    print(f"[INFO] Uploaded {len(df_country)} countries to {DATASET_ID}.{TABLE_ID}")