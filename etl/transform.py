import pandas as pd
from pathlib import Path
import logging

logging.basicConfig(level=logging.INFO, format="%(message)s")


def load_txt_files(raw_dir: Path):
    """
    Load all .txt FAERS files from a directory into a dictionary of DataFrames.

    Parameters
    ----------
    raw_dir : Path
        Directory containing .txt files.

    Returns
    -------
    dict
        {filename_stem: DataFrame} for all TXT files in raw_dir
    """
    dfs = {}
    for txt_file in raw_dir.glob("*.txt"):
        dfs[txt_file.stem] = pd.read_csv(txt_file, sep="$", dtype=str)
#        demo = pd.read_csv('merged_demo.csv', dtype={'to_mfr': str}, low_memory=False)

        logging.info(f"Loaded {txt_file.name} → {txt_file.stem}")
    return dfs


def merge_and_save_all_tables(dfs_dict: dict, output_dir: Path):
    """
    Dynamically merges all quarters for each FAERS table in dfs_dict
    and saves a single CSV per table in output_dir.
    Handles any number of quarters automatically.

    Parameters
    ----------
    dfs_dict : dict
        {filename_stem: DataFrame}
    output_dir : Path
        Directory to save merged CSVs

    Returns
    -------
    dict
        {table_name: merged DataFrame}
    """
    merged_dfs = {}
    output_dir.mkdir(parents=True, exist_ok=True)

    # Determine all unique table prefixes (before the quarter part)
    table_prefixes = set(key[:key.find("25")] for key in dfs_dict.keys())

    for prefix in table_prefixes:
        # Select all quarters for this table
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.startswith(prefix)]
        merged_df = pd.concat(table_dfs, ignore_index=True)
        merged_dfs[prefix] = merged_df

        # Save merged CSV
        output_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(output_file, index=False)
        logging.info(f"Merged {prefix}: {len(table_dfs)} files → {output_file}")

    return merged_dfs


