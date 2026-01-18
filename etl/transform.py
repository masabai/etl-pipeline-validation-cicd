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
    
