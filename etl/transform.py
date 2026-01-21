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
# ------------------ Memory-Safe Merge + Transform ------------------ #
def merge_and_transform_one_by_one(raw_dir, output_dir):
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. Clean up old files first
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()
        logging.info(f"Deleted old file: {f.name}")

    # 2. Identify all unique table prefixes (e.g., 'DEMO', 'DRUG')
    all_files = list(raw_dir.glob("*.txt"))
    table_prefixes = set(f.stem[:-4] for f in all_files) # Removes '25Q1', etc.

    for prefix in table_prefixes:
        logging.info(f">>> Processing Group: {prefix}")
        
        # 3. Load and merge ONLY the files for this specific prefix
        # This keeps RAM low by only holding one group at a time
        current_group_dfs = []
        for f in raw_dir.glob(f"{prefix}*.txt"):
            df = pd.read_csv(f, sep="$", dtype=str, low_memory=False)
            current_group_dfs.append(df)
            logging.info(f"  Loaded {f.name}")

        if not current_group_dfs:
            continue

        merged_df = pd.concat(current_group_dfs, ignore_index=True)
        
        # Clear the list of individual DataFrames to free RAM immediately
        del current_group_dfs 

        # 4. Apply transformations
        if prefix.upper() == 'DEMO':
            merged_df = transform_demo(merged_df)
        elif prefix.upper() == 'DRUG':
            merged_df = transform_drug(merged_df)
        else:
            merged_df = transform_generic(merged_df, prefix)

        # 5. Save and immediately clear from memory
        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        logging.info(f"Saved: {out_file.name} ({len(merged_df)} rows)")
        
        del merged_df # Critical: Free RAM before the next prefix starts
