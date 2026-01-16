# etl/pipeline.py

from pathlib import Path
import logging
import os
from etl.extract import download_faers_data
#from validation.extract_gx import validate_all_texts
from etl.transform import load_txt_files, merge_and_save_all_tables

"""
from db.snowflake_conn import get_snowflake_connection

# Skip load function if RUN_SNOWFLAKE_LOAD =0
if os.environ.get("RUN_SNOWFLAKE_LOAD") == "1":
    from etl.load import load_csv_to_snowflake  # uses single connection
"""

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------- Directories ----------------
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

# ---------------- Extract ----------------
downloaded_files = download_faers_data(raw_dir=RAW_DIR)
logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")

# ---------------- GX Validation ----------------
# validate_all_texts()

# ---------------- Transform ----------------
dfs = load_txt_files(RAW_DIR)
merge_and_save_all_tables(dfs, PROCESSED_DIR)

"""
# ---------------- Load (toggleable) ----------------
if os.environ.get("RUN_SNOWFLAKE_LOAD") == "1":
    logging.info("Snowflake load enabled. Connecting...")
    conn = get_snowflake_connection()
    logging.info("Connected to Snowflake")

    try:
        for csv_file in PROCESSED_DIR.glob("merged_*.csv"):
            table_name = csv_file.stem.replace("merged_", "").upper()
            logging.info(f"Loading {csv_file.name} → {table_name}")

            rows_inserted = load_csv_to_snowflake(
                conn=conn,
                csv_path=csv_file,
                table=table_name
            )
            logging.info(f"{table_name} loaded, rows inserted: {rows_inserted}")

        # ---------------- Verify ----------------
        cs = conn.cursor()
        for table in ["DEMO", "DRUG", "INDI", "OUTC", "REAC", "RPSR", "THER"]:
            cs.execute(f"SELECT COUNT(*) FROM {table}")
            logging.info(f"{table} rows: {cs.fetchone()[0]}")
        cs.close()

    finally:
        conn.close()
        logging.info("Snowflake connection closed")
else:
    logging.info("Snowflake load skipped (RUN_SNOWFLAKE_LOAD not set)")
"""

print("--- Full ETL pipeline complete ---")