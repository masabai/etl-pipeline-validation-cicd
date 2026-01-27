import pandas as pd
from pathlib import Path
import logging
from datetime import datetime
import gc

logging.basicConfig(level=logging.INFO, format="%(message)s")

def load_txt_files(raw_dir):
    """
    Load all TXT files in a directory into Pandas DataFrames.

    Args:
        raw_dir: Directory containing raw TXT files.

    Returns:
        Mapping from file stem to loaded DataFrame.
    """
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        dfs[txt_file.stem] = pd.read_csv(
            txt_file, sep="$", dtype=str, low_memory=False
        )
        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs


def clean_common_fields(df):
    """
    Apply common cleaning steps to any FAERS DataFrame.

    Args:
        df: Input DataFrame.

    Returns:
        Cleaned DataFrame.
    """
    df = df.copy()

    df.columns = [c.lower().strip() for c in df.columns]
    df = df.drop_duplicates()

    if "primaryid" in df.columns and "caseid" in df.columns:
        df = df[~df[["primaryid", "caseid"]].isnull().all(axis=1)]

    for col in df.select_dtypes(include="object").columns:
        df[col] = df[col].fillna("Unknown")

    if "primaryid" in df.columns:
        df["primaryid"] = df["primaryid"].astype(str).str.strip()
    if "caseid" in df.columns:
        df["caseid"] = df["caseid"].astype(str).str.strip()

    return df


def transform_demo(df):
    """
    Transform DEMO dataset with FAERS-specific cleaning.

    Args:
        df: Raw DEMO DataFrame.

    Returns:
        Transformed DataFrame.
    """
    df = df.copy()
    df["load_ts"] = datetime.now()

    if "age" in df.columns:
        df["age"] = pd.to_numeric(df["age"], errors="coerce")
    if "wt" in df.columns:
        df["wt"] = pd.to_numeric(df["wt"], errors="coerce")
    if "sex" in df.columns:
        df["sex"] = df["sex"].str.upper().str.strip()

    for col in df.columns:
        if "dt" in col or "date" in col:
            df[col] = pd.to_datetime(df[col], errors="coerce")

    df = clean_common_fields(df)
    return df


def transform_drug(df):
    """
    Transform DRUG dataset with FAERS-specific cleaning.

    Args:
        df: Raw DRUG DataFrame.

    Returns:
        Transformed DataFrame.
    """
    df = df.copy()
    df["load_ts"] = datetime.now()

    if "drugname" in df.columns:
        df["drugname"] = df["drugname"].str.upper().str.strip()
    if "role_cod" in df.columns:
        df["role_cod"] = df["role_cod"].str.upper().str.strip()

    df = clean_common_fields(df)
    return df


def transform_generic(df, table_name):
    """
    Apply generic transformation for tables other than DEMO/DRUG.

    Args:
        df: Raw DataFrame.
        table_name: Name of the table being transformed.

    Returns:
        Transformed DataFrame.
    """
    df = df.copy()
    df["load_ts"] = datetime.now()
    df = clean_common_fields(df)
    return df


def merge_and_transform_one_by_one(raw_dir, output_dir):
    """
    Merge and transform FAERS TXT files group by group in a memory-safe way.

    Args:
        raw_dir: Directory containing raw TXT files.
        output_dir: Directory where merged CSVs will be saved.
    """
    output_dir.mkdir(parents=True, exist_ok=True)

    for f in output_dir.glob("merged_*.csv"):
        f.unlink()

    all_files = list(raw_dir.glob("*.txt"))
    table_prefixes = set(f.stem[:-4] for f in all_files)

    for prefix in table_prefixes:
        logging.info(f">>> Processing Group: {prefix}")
        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        first_chunk = True

        for f in raw_dir.glob(f"{prefix}*.txt"):
            logging.info(f"  Streaming {f.name}...")

            chunk_iter = pd.read_csv(f, sep="$", dtype=str, low_memory=True, chunksize=100000)

            for chunk in chunk_iter:
                if prefix.upper() == 'DEMO':
                    chunk = transform_demo(chunk)
                elif prefix.upper() == 'DRUG':
                    chunk = transform_drug(chunk)
                else:
                    chunk = transform_generic(chunk, prefix)

                chunk.to_csv(out_file, mode='a', index=False, header=first_chunk)
                first_chunk = False

                del chunk
                gc.collect()

        logging.info(f"Successfully finalized: {out_file.name}")
