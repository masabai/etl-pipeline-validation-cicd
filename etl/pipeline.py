from pathlib import Path
import logging
from etl.extract import download_faers_data
# from validation.extract_gx import validate_all_texts
# from etl.transform import load_txt_files, merge_and_save_all_tables
import warnings

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

# ---------------- Transform / Validation placeholders ----------------
# Example: later you can use PROCESSED_DIR for outputs
# merged_data = merge_and_save_all_tables(RAW_DIR, PROCESSED_DIR)
# validate_all_texts(PROCESSED_DIR)
