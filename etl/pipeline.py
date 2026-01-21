from pathlib import Path
import logging
from etl.extract import download_faers_data
from validation.extract_gx import validate_all_texts
from etl.transform import load_txt_files, merge_and_transform_one_by_one
from etl.load import load_csv_to_snowflake
from db.snowflake_conn import get_snowflake_connection
import warnings
import os

# ---------------- Setup ----------------
logging.basicConfig(level=logging.INFO, format="%(message)s")
warnings.filterwarnings("ignore")

# ---------------- Directories ----------------
# BASE_DIR points to repo root in Codespaces (default working dir)
BASE_DIR = Path.cwd()  

# Raw and processed data
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

# Logs folder
LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

# Tests folder
TESTS_DIR = BASE_DIR / "tests"
TESTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Extract ----------------
downloaded_files = download_faers_data(raw_dir=RAW_DIR)
logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")

# ---------------- Transform ----------------
#dfs = load_txt_files(RAW_DIR)
merge_and_transform_one_by_one(RAW_DIR, PROCESSED_DIR) 

# ---------------- Gx ----------------
validate_all_texts(PROCESSED_DIR)

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

print("--- Full ETL pipeline complete ---")


import subprocess

def run_dbt():
    logging.info("Starting dbt transformations...")
    # Runs dbt models and dbt tests (data build tool's internal tests)
    subprocess.run(["dbt", "run"], check=True)
    subprocess.run(["dbt", "test"], check=True)
"""