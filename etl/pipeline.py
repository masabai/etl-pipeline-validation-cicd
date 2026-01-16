# etl/pipeline.py

from pathlib import Path
import logging
import os
from etl.extract import download_faers_data
from etl.transform import load_txt_files, merge_and_transform_one_by_one, transform_demo, transform_generic

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
merge_and_transform_one_by_one(dfs, PROCESSED_DIR)

# ---------------- Load ----------------
"""
# Your fully-tested load code can remain here
if os.environ.get("RUN_SNOWFLAKE_LOAD") == "1":
    from db.snowflake_conn import get_snowflake_connection
    from etl.load import load_csv_to_snowflake
    ...
"""

print("--- Full ETL pipeline complete ---")
