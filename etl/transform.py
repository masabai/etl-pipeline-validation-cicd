# etl/transform.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")


def load_txt_files(raw_dir: Path):
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        dfs[txt_file.stem] = pd.read_csv(txt_file, sep="$", dtype=str, low_memory=False)
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs


def transform_demo(df):
    # simple demo transformation
    df = df.copy()
    df['load_ts'] = datetime.now()
    df = df.drop_duplicates()
    return df


def transform_generic(df, table_name: str):
    df = df.copy()
    df['load_ts'] = datetime.now()
    return df


def merge_and_transform_one_by_one(dfs_dict: dict, output_dir: Path):
    """
    Merge quarters and transform one table at a time (memory safe)
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    # Clear old merged CSVs
    for f in output_dir.glob("merged_*.csv"):
        f.unlink()
        logging.info(f"Deleted old file: {f.name}")

    # Determine table prefixes (before quarter, e.g., DEMO25Q1 -> DEMO)
    table_prefixes = set(k[:-4] for k in dfs_dict.keys())  # remove last 5 chars: 25Q1/25Q2

    for prefix in table_prefixes:
        # Merge all quarters for this table
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.startswith(prefix)]
        merged_df = pd.concat(table_dfs, ignore_index=True)

        # Transform
        if prefix.upper() == 'DEMO':
            merged_df = transform_demo(merged_df)
        else:
            merged_df = transform_generic(merged_df, prefix)

        # Save CSV
        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        logging.info(f"Saved transformed table: {out_file} ({len(merged_df)} rows)")

    logging.info("All tables merged & transformed successfully.")
