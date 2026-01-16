# etl/pipeline.py

from pathlib import Path
import logging
import os
from etl.extract import download_faers_data
from etl.transform import load_txt_files, merge_and_save_all_tables, transform_demo, transform_generic

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------- Directories ----------------
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

# ---------------- Extract ----------------
downloaded_files = download_faers_data(raw_dir=RAW_DIR)
logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")

# ---------------- Transform ----------------
dfs = load_txt_files(RAW_DIR)
merged_dfs = merge_and_save_all_tables(dfs, PROCESSED_DIR)

# Process & save each table one at a time (memory safe)
for table_name, df in merged_dfs.items():
    if table_name.lower() == 'demo':
        df_t = transform_demo(df)
    else:
        df_t = transform_generic(df, table_name)

    out_file = PROCESSED_DIR / f"merged_{table_name.lower()}.csv"
    df_t.to_csv(out_file, index=False)
    logging.info(f"Saved transformed table: {out_file} ({len(df_t)} rows)")

# ---------------- Load ----------------
"""
# Your fully-tested load code can remain here
if os.environ.get("RUN_SNOWFLAKE_LOAD") == "1":
    from db.snowflake_conn import get_snowflake_connection
    from etl.load import load_csv_to_snowflake
    ...
"""

print("--- Full ETL pipeline complete ---")
