from pathlib import Path
import logging
from etl.extract import download_faers_data
# from validation.extract_gx import validate_all_texts
from etl.transform import load_txt_files, merge_and_transform_one_by_one
#from etl.load import load_csv_to_snowflake
#from db.snowflake_conn import get_snowflake_connection
import warnings
import pandas as pd

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

# ---------------- Extract ----------------
downloaded_files = download_faers_data(raw_dir=RAW_DIR)
demo = pd.read_csv(RAW_DIR / "DEMO25Q1.txt", sep="$", dtype=str, low_memory=False)
drug = pd.read_csv(RAW_DIR / "DRUG25Q1.txt", sep="$", dtype=str, low_memory=False)
print(demo.info())
#print(drug.info())