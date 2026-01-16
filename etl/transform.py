# etl/transform.py

import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")


# Load TXT files into DataFrames
def load_txt_files(raw_dir):
    """
    Load all .txt FAERS files from a directory into a dictionary of DataFrames.
    """
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        df = pd.read_csv(txt_file, sep="$", dtype=str, low_memory=False)
        dfs[txt_file.stem] = df
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs


# Merge quarters and save CSVs
def merge_and_save_all_tables(dfs_dict, output_dir):
    """
    Merge all quarters for each FAERS table in dfs_dict and save a single CSV per table.
    """
    merged_dfs = {}
    output_dir.mkdir(parents=True, exist_ok=True)

    table_prefixes = set(key[:key.find("25")] for key in dfs_dict.keys())

    for prefix in table_prefixes:
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.startswith(prefix)]
        merged_df = pd.concat(table_dfs, ignore_index=True)
        merged_dfs[prefix] = merged_df

        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        logging.info(f"Merged {prefix}: {len(table_dfs)} files → {out_file}")

    return merged_dfs


# Base transform for all tables
def base_transform(df, source_table, load_ts=None):
    """
    Minimal, testing-friendly transformations:
    - primaryid and caseid as string
    - drop rows missing primary keys
    - add audit columns
    - optional category conversion for object columns
    """
    if load_ts is None:
        load_ts = datetime.utcnow()

    df['primaryid'] = df['primaryid'].astype(str)
    df['caseid'] = df['caseid'].astype(str)
    df = df.dropna(subset=['primaryid', 'caseid'])

    df['etl_loaded_at'] = load_ts
    df['source_table'] = source_table

    for col in df.select_dtypes(include='object').columns:
        if df[col].nunique() < 1000:
            df[col] = df[col].astype('category')

    return df


# Demo table: parse dates, numeric columns, deduplicate
def transform_demo(df):
    df = base_transform(df, 'demo')

    date_cols = ['event_dt', 'mfr_dt', 'init_fda_dt', 'fda_dt', 'rept_dt']
    for col in date_cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce', format='%Y%m%d')

    num_cols = ['age', 'wt', 'caseversion', 'primaryid']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors='coerce')

    if 'caseid' in df.columns and 'caseversion' in df.columns:
        df = df.sort_values(['caseid', 'caseversion'], ascending=[True, False])
        df = df.drop_duplicates(subset=['caseid'], keep='first')

    return df


# Other tables: base transform only
def transform_generic(df, table_name):
    return base_transform(df, table_name)


# Apply transforms to all merged tables
def transform_all(merged_dfs):
    transformed_dfs = {}
    for table_name, df in merged_dfs.items():
        if table_name.lower() == 'demo':
            transformed_dfs[table_name] = transform_demo(df)
        else:
            transformed_dfs[table_name] = transform_generic(df, table_name)
        logging.info(f"Transformed table: {table_name} ({len(transformed_dfs[table_name])} rows)")

    return transformed_dfs





