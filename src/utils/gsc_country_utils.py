#!/usr/bin/env python3
# -*- coding: utf-8 -*-
# ============================================================
# File: gsc_country_utils.py
# Revision: Rev.1 - Adding Kosovo country
# Purpose: Converting ISO 3166 Alpha-2 / Alpha-3 Codes or names
#          to full Country Name using BigQuery reference table.
# ============================================================

from google.cloud import bigquery
import pandas as pd

# =================================================
# Function: load_country_map
# =================================================
def load_country_map(project: str, dataset: str, table: str) -> dict:
    """
    Load GSC country dimension from BigQuery and return a dictionary for mapping
    country_code (ISO Alpha-2) -> country_name (full name)
    """
    client = bigquery.Client()
    query = f"""
        SELECT country_code, country_name
        FROM `{project}.{dataset}.{table}`
    """
    df = client.query(query).to_dataframe()
    
    # Ensure uppercase ISO Alpha-2
    df['country_code'] = df['country_code'].str.upper()
    
    return dict(zip(df['country_code'], df['country_name']))

# =================================================
# Function: map_country_column
# =================================================
def map_country_column(df: pd.DataFrame, country_col: str, country_map: dict, new_col: str = "country") -> pd.DataFrame:
    """
    Map a DataFrame column containing country codes to full country names
    """
    df[country_col] = df[country_col].str.upper()
    df[new_col] = df[country_col].map(country_map)
    return df

# =================================================
# Function: robust_map_country_column
# =================================================
def robust_map_country_column(
    df: pd.DataFrame, 
    country_col: str, 
    country_map: dict, 
    new_col: str = "Country"
) -> pd.DataFrame:
    """
    Robust mapping for GSC country values:
      - Handles both Alpha-2 and Alpha-3 codes directly from the map
      - Handles 'zzz', 'zz', or 'unknown' as 'Unknown Region'
      - Handles 'XKK' as Kosovo
      - Non-mapped values return None
    """
    if df is None or df.empty or country_map is None:
        return df

    def map_one(val):
        if pd.isna(val):
            return None
        s = str(val).strip().upper()
        if s in ("", "ZZ", "ZZZ", "UNKNOWN"):
            return "Unknown Region"
        if s == "XKK":
            return "Kosovo"
        return country_map.get(s, "__NO_COUNTRY__")

    df[new_col] = df[country_col].apply(map_one)
    return df
