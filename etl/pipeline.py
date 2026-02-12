"""
FDA ETL Pipeline Runner

This script orchestrates the end-to-end FDA FAERS ETL workflow:
- Extracts raw FAERS ZIP data from FDA servers
- Transforms and merges raw tables into processed CSVs
- Validates processed data using Great Expectations
- Optionally loads processed CSVs into Snowflake
- Executes local dbt transformations and tests

Designed for reproducible local runs and CI/CD integration.

Date: 2026-02-05
"""

from pathlib import Path
import logging
import warnings
import os
import subprocess

from etl.extract import download_faers_data
from validation.extract_gx import validate_all_texts
from etl.transform import merge_and_transform_one_by_one
from etl.load import load_csv_to_snowflake
from db.snowflake_conn import get_snowflake_connection

logging.basicConfig(level=logging.INFO, format="%(message)s")
warnings.filterwarnings("ignore")

# ----------------------------------------
# Setup directories
# ----------------------------------------
BASE_DIR = Path.cwd()
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TESTS_DIR = BASE_DIR / "tests"
TESTS_DIR.mkdir(parents=True, exist_ok=True)


def run_etl():
    """
    Execute the full FDA ETL pipeline:
    1. Extract raw FAERS data
    2. Transform and merge into processed CSVs
    3. Validate processed data via Great Expectations
    4. Optionally load into Snowflake
    5. Run local dbt transformations and tests
    """
    # ---------------- Extract ---------------- #
    downloaded_files = download_faers_data(raw_dir=RAW_DIR)
    logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")

    # ---------------- Transform ---------------- #
    merge_and_transform_one_by_one(RAW_DIR, PROCESSED_DIR)

    # ---------------- Validation ---------------- #
    validate_all_texts(PROCESSED_DIR)
    logging.info("Great Expectations validation complete.")

    # ---------------- Load to Snowflake ---------------- #
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

            # Optional verification
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

def run_dbt():
    """
    Run dbt dependencies, models, and tests.
    Controlled by environment variable RUN_DBT for CI/CD.
    """
    if os.environ.get("RUN_DBT") != "1":
        logging.info("DBT run skipped (RUN_DBT not set)")
        return

    try:
        # 1. ALWAYS install dependencies first
        logging.info("Installing dbt dependencies...")
        subprocess.run(["dbt", "deps"], check=True)

        # 2. Run the models
        logging.info("Starting dbt transformations...")
        subprocess.run(["dbt", "run"], check=True)

        # 3. Run the tests
        logging.info("Starting dbt tests...")
        subprocess.run(["dbt", "test"], check=True)
        
        logging.info("DBT run and tests completed successfully.")
    except subprocess.CalledProcessError as e:
        logging.error(f"DBT execution failed: {e}")
        raise

if __name__ == "__main__":
    run_etl()
    run_dbt()
    logging.info("--- Full ETL pipeline complete ---")
