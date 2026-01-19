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
        dfs[txt_file.stem] = pd.read_csv(txt_file, sep="$", dtype=str, low_memory=False)
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs

# ------------------ Cleaning ------------------ #
def clean_table(df, table_name):
    df = df.copy()

    # Standardize column names
    df.columns = [c.lower().strip() for c in df.columns]

    # Drop rows where primaryid or caseid are null
    if 'primaryid' in df.columns and 'caseid' in df.columns:
        df = df.dropna(subset=['primaryid', 'caseid'])

    # Drop duplicates
    df = df.drop_duplicates()

    # Strip strings
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Type conversion for IDs
    for col in ['primaryid', 'caseid']:
        if col in df.columns:
            df[col] = df[col].astype(str)

    # Normalize date columns (any column containing 'dt')
    for col in df.columns:
        if 'dt' in col:
            try:
                df[col] = pd.to_datetime(df[col], errors='coerce')
            except Exception:
                pass

    # Table-specific optional mappings/fills
    if table_name == 'DEMO':
        if 'sex' in df.columns:
            df['sex'] = df['sex'].replace({'M': 'Male', 'F': 'Female'}).fillna('Unknown')
        if 'age_grp' in df.columns:
            df['age_grp'] = df['age_grp'].fillna('Unknown')
    elif table_name == 'DRUG':
        if 'dose_unit' in df.columns:
            df['dose_unit'] = df['dose_unit'].fillna('UNKNOWN')
        if 'exp_dt' in df.columns:
            df['exp_dt'] = pd.to_datetime(df['exp_dt'], errors='coerce')

    # Add batch metadata
    df['load_ts'] = datetime.now()

    return df

# ------------------ Table-specific Transform ------------------ #
def transform_demo(df):
    return clean_table(df, 'DEMO')

def transform_generic(df, table_name):
    return clean_table(df, table_name)

# ------------------ Merge & Transform ------------------ #
def merge_and_transform_one_by_one(dfs_dict: dict, output_dir: Path):
    """
    Merge quarters and transform one table at a time (memory safe)
    Cleanup happens ONCE before loop to avoid deleting files mid-run.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # -------------------- CLEANUP (Run once!) -------------------- #
    logging.info("Cleaning processed directory...")
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()
        logging.info(f"Deleted old file: {f.name}")

    # -------------------- MERGE & TRANSFORM -------------------- #
    TABLES = ["DEMO", "DRUG", "REAC", "THER", "INDI", "OUTC", "RPSR"]

    for table in TABLES:
        # Gather all available quarters for this table
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.upper().startswith(table)]
        if not table_dfs:
            logging.warning(f"⚠️ No raw files found for table {table}, skipping")
            continue

        merged_df = pd.concat(table_dfs, ignore_index=True)
        merged_df = transform_demo(merged_df) if table == "DEMO" else transform_generic(merged_df, table)

        # Save CSV
        out_file = output_dir / f"merged_{table.lower()}.csv"
        merged_df.to_csv(out_file, index=False)

        # Logging summary
        before_rows = sum(len(df) for df in table_dfs)
        after_rows = len(merged_df)
        dropped = before_rows - after_rows
        logging.info(f"{table} → before: {before_rows}, after: {after_rows}, dropped: {dropped}")
        logging.info(f"{table} staged → {after_rows} rows")

    logging.info("All tables merged & transformed successfully.")

 