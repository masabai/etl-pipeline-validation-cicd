# etl/transform.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ------------------ Load TXT Files ------------------ #
def load_txt_files(raw_dir: Path):
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        # low_memory=False is best practice for FAERS 2026 data volume
        dfs[txt_file.stem] = pd.read_csv(txt_file, sep="$", dtype=str, low_memory=False)
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs

<<<<<<< HEAD
# ------------------ The "Full" Cleaning Logic ------------------ #
def clean_table(df, table_name):
    """
    Applied to all 7 tables: Handles nulls, duplicates, strings, types, and mappings.
    """
    df = df.copy() # Keeps function 'pure' and prevents side effects

    # 1. Standardize column names (lowercase and no whitespace)
    df.columns = [c.lower().strip() for c in df.columns]

    # 2. Drop if primaryid or caseid are null (Crucial for FAERS integrity)
    id_cols = [c for c in ['primaryid', 'caseid'] if c in df.columns]
    if id_cols:
        df = df.dropna(subset=id_cols)
=======
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
>>>>>>> a190c5652d681a15096ba0ddf3da49c8e16a2875

    # 3. Drop duplicates
    df = df.drop_duplicates()

    # 4. String cleaning (strip whitespace from all text)
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # 5. Type conversion for IDs (Ensure they are strings for database consistency)
    for col in id_cols:
        df[col] = df[col].astype(str)

    # 6. Normalize date columns (anything containing 'dt')
    for col in df.columns:
        if 'dt' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # 7. Table-specific mappings/fills
    if table_name == 'DEMO':
        if 'sex' in df.columns:
            df['sex'] = df['sex'].replace({'M': 'Male', 'F': 'Female'}).fillna('Unknown')
    elif table_name == 'DRUG':
        if 'dose_unit' in df.columns:
            df['dose_unit'] = df['dose_unit'].fillna('UNKNOWN')

    # 8. Add batch metadata
    df['load_ts'] = datetime.now()

    return df

# ------------------ Merge & Transform ------------------ #
def merge_and_transform_one_by_one(dfs_dict: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    # Cleanup processed folder ONCE at the start
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()

    # Use your proven 7-table logic from yesterday
    BASE_TABLES = ["DEMO", "DRUG", "REAC", "THER", "INDI", "OUTC", "RPSR"]

    for table in BASE_TABLES:
        # Match keys (e.g., DEMO matches DEMO25Q1 and DEMO25Q2)
        matching_keys = [k for k in dfs_dict.keys() if k.upper().startswith(table)]
        
        if not matching_keys:
            logging.warning(f"⚠️ Skipping {table}: No files found.")
            continue

        # Merge quarters
        merged_df = pd.concat([dfs_dict[k] for k in matching_keys], ignore_index=True)

        # Apply the FULL cleaning logic
        before_count = len(merged_df)
        merged_df = clean_table(merged_df, table)
        after_count = len(merged_df)

        # Save CSV
        out_file = output_dir / f"merged_{table.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        
        logging.info(f"{table} → Merged {len(matching_keys)} files. Rows: {before_count} -> {after_count} (Dropped {before_count - after_count})")

    logging.info("All 7 tables cleaned and merged successfully.")

