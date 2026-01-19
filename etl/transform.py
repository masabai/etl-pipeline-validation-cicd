import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ------------------ Load TXT Files ------------------ #
def load_txt_files(raw_dir: Path):
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        # low_memory=False is recommended for 2026 FAERS datasets to avoid DtypeWarnings
        dfs[txt_file.stem] = pd.read_csv(txt_file, sep="$", dtype=str, low_memory=False)
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs

# ------------------ Cleaning ------------------ #
def clean_table(df, table_name):
    # Pandas 3.0+ uses Copy-on-Write by default, but explicit .copy() 
    # remains best practice for pipeline clarity.
    df = df.copy()

    df.columns = [c.lower().strip() for c in df.columns]

    # Mandatory FAERS deduplication: drops records with missing IDs
    if 'primaryid' in df.columns and 'caseid' in df.columns:
        df = df.dropna(subset=['primaryid', 'caseid'])

    # Deduplicate rows: This explains the ~131 row drop in DEMO
    df = df.drop_duplicates()

    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()

    # Normalize Dates
    for col in df.columns:
        if 'dt' in col:
            df[col] = pd.to_datetime(df[col], errors='coerce')

    # Table-specific normalization
    if table_name == 'DEMO':
        if 'sex' in df.columns:
            df['sex'] = df['sex'].replace({'M': 'Male', 'F': 'Female'}).fillna('Unknown')
    
    df['load_ts'] = datetime.now()
    return df

# ------------------ Merge & Transform ------------------ #
def merge_and_transform_one_by_one(dfs_dict: dict, output_dir: Path):
    output_dir.mkdir(parents=True, exist_ok=True)

    # 1. CLEANUP (Run once, outside the loop)
    logging.info("Cleaning processed directory...")
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()

    # 2. TARGET TABLES
    TABLES = ["DEMO", "DRUG", "REAC", "THER", "INDI", "OUTC", "RPSR"]

    for table in TABLES:
        # FIX: Use 'in' rather than 'startswith' to catch filenames like 'DEMO25Q1'
        table_dfs = [dfs_dict[k] for k in dfs_dict if table in k.upper()]
        
        if not table_dfs:
            logging.warning(f"⚠️ No raw files found for table {table}, skipping")
            continue

        merged_df = pd.concat(table_dfs, ignore_index=True)
        
        # Apply transformation
        merged_df = clean_table(merged_df, table)

        # 3. SAVE CSV with unique filename
        out_file = output_dir / f"merged_{table.lower()}.csv"
        merged_df.to_csv(out_file, index=False)

        # Logging summary
        before_rows = sum(len(df) for df in table_dfs)
        after_rows = len(merged_df)
        logging.info(f"{table} → before: {before_rows}, after: {after_rows}, dropped: {before_rows - after_rows}")

    logging.info("All tables merged & transformed successfully.")
