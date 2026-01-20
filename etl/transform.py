# etl/transform.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")


# ------------------ Load TXT Files ------------------ #
def load_txt_files(raw_dir):
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        dfs[txt_file.stem] = pd.read_csv(
            txt_file, sep="$", dtype=str, low_memory=False
        )
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs

# ------------------ Cleaning Functions ------------------ #
def clean_common_fields(df):
    df = df.copy()

    # Clean column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Drop duplicates
    df = df.drop_duplicates()

    # Drop rows only if BOTH primaryid & caseid are null
    if "primaryid" in df.columns and "caseid" in df.columns:
        df = df[~df[["primaryid", "caseid"]].isnull().all(axis=1)]

    # Fill missing strings
    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("Unknown")

    # Normalize IDs
    if "primaryid" in df.columns:
        df["primaryid"] = df["primaryid"].astype(str).str.strip()
    if "caseid" in df.columns:
        df["caseid"] = df["caseid"].astype(str).str.strip()

    return df


def transform_demo(df):
    df = df.copy()
    df["load_ts"] = datetime.now()

    # DEMO dataset - specific cleaning
    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
    if "wt" in df.columns:
        df["wt"] = pd.to_numeric(df["wt"], errors="coerce")
    if "sex" in df.columns:
        df["sex"] = df["sex"].str.upper().str.strip()

    # Normalize date columns
    for col in df.columns:
        if "dt" in col or "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = clean_common_fields(df)
    return df


def transform_drug(df):
    df = df.copy()
    df["load_ts"] = datetime.now()

    # DRUG-specific cleaning
    if "drugname" in df.columns:
        df["drugname"] = df["drugname"].str.upper().str.strip()
    if "role_cod" in df.columns:
        df["role_cod"] = df["role_cod"].str.upper().str.strip()

    df = clean_common_fields(df)
    return df


def transform_generic(df, table_name):
    df = df.copy()
    df["load_ts"] = datetime.now()
    df = clean_common_fields(df)
    return df


# ------------------ Merge + Transform ------------------ #
def merge_and_transform_one_by_one(dfs_dict, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    # Remove old merged CSVs
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()
        logging.info(f"Deleted old file: {f.name}")

    # Determine table prefixes (before quarter, e.g., DEMO25Q1 -> DEMO)
    table_prefixes = set(k[:-4] for k in dfs_dict.keys())  # remove last 5 chars: 25Q1/25Q2

    for prefix in table_prefixes:
        # Merge all quarters for this table
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.startswith(prefix)]
        merged_df = pd.concat(table_dfs, ignore_index=True)

        rows_before = len(merged_df)

        # Transform
        if prefix.upper() == 'DEMO':
            merged_df = transform_demo(merged_df)
        else:
            merged_df = transform_generic(merged_df, prefix)

        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        logging.info(f"Saved transformed table: {out_file} ({len(merged_df)} rows)")

    logging.info("All tables merged & transformed successfully.")
