# etl/pipeline.py

from pathlib import Path
import logging
import os
from etl.extract import download_faers_data
from etl.transform import load_txt_files, merge_and_save_all_tables, transform_all

logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------- Directories ----------------
RAW_DIR = Path("data/raw")
RAW_DIR.mkdir(parents=True, exist_ok=True)
PROCESSED_DIR = Path(__file__).resolve().parent.parent / "data" / "processed"

# ---------------- Extract ----------------
downloaded_files = download_faers_data(raw_dir=RAW_DIR)
logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")

# ---------------- GX Validation ----------------
# validate_all_texts()  # optional

# ---------------- Transform ----------------
dfs = load_txt_files(RAW_DIR)
merged_dfs = merge_and_save_all_tables(dfs, PROCESSED_DIR)
transformed_dfs = transform_all(merged_dfs)

# Save transformed tables (overwrite merged CSVs)
for table_name, df in transformed_dfs.items():
    out_file = PROCESSED_DIR / f"merged_{table_name.lower()}.csv"
    df.to_csv(out_file, index=False)
    logging.info(f"Saved transformed table: {out_file} ({len(df)} rows)")

# ---------------- Load ----------------
"""
# Load is fully tested — keep your existing code here
if os.environ.get("RUN_SNOWFLAKE_LOAD") == "1":
    from db.snowflake_conn import get_snowflake_connection
    from etl.load import load_csv_to_snowflake

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

        # Verify row counts
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
