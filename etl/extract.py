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
BASE_DIR = Path.cwd()  # repo root

RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

LOGS_DIR = BASE_DIR / "logs"
LOGS_DIR.mkdir(parents=True, exist_ok=True)

TESTS_DIR = BASE_DIR / "tests"
TESTS_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- Extract ----------------
# Check if RAW_DIR has any files
existing_files = list(RAW_DIR.glob("*"))
if existing_files:
    logging.info(f"Raw data already exists. Skipping download. Files: {[f.name for f in existing_files]}")
    downloaded_files = existing_files
else:
    downloaded_files = download_faers_data(raw_dir=RAW_DIR)
    logging.info(f"Extract complete. Files: {[f.name for f in downloaded_files]}")
