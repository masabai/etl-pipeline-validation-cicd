import logging
from pathlib import Path
import requests
import zipfile
import io
import time

logging.basicConfig(level=logging.INFO, format="%(message)s")

BASE_DIR = Path.cwd()  # repo root
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# FAERS ZIP URLs 
FAERS_URLS = {
    "Q1_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q1.zip",
    "Q2_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q2.zip"
}

# FAERS tables 
FAERS_TABLES = ["DEMO25Q", "DRUG25Q", "REAC25Q",
                "OUTC25Q", "RPSR25Q", "INDI25Q", "THER25Q"]


def download_faers_data(raw_dir: Path):
    """
    Download FAERS ZIPs for 2025 Q1 and Q2, extract all relevant tables,
    and save as .txt files in raw_dir.
    Retries up to 3 times on network errors.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Skip download if all files exist
    existing_files = list(raw_dir.glob("*.txt"))
    if len(existing_files) >= 14:
        logging.info("All 14 FAERS TXT files already exist — skipping download")
        return existing_files

    downloaded_files = []

    for quarter, url in FAERS_URLS.items():
        logging.info(f"Downloading {quarter} ...")

        for attempt in range(3):
            try:
                # Stream download to handle large files
                with requests.get(url, stream=True, timeout=120, verify=False) as r:
                    r.raise_for_status()
                    buffer = io.BytesIO()
                    for chunk in r.iter_content(chunk_size=8192):
                        buffer.write(chunk)
                    buffer.seek(0)

                    # Open ZIP from memory
                    with zipfile.ZipFile(buffer) as z:
                        logging.info(f"ZIP contents: {z.namelist()}")
                        for f in z.namelist():
                            fname = f.split("/")[-1]
                            if fname.endswith(".txt") and any(fname.startswith(t) for t in FAERS_TABLES):
                                out_path = raw_dir / fname
                                if not out_path.exists():
                                    with z.open(f) as src, open(out_path, "wb") as out:
                                        out.write(src.read())
                                    logging.info(f"Downloaded '{fname}'")
                                else:
                                    logging.info(f"File '{fname}' already exists. Skipping.")
                                downloaded_files.append(out_path)
                break  # success, exit retry loop

            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    logging.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logging.error(f"Failed to download {quarter} after 3 attempts")
                    raise

    return downloaded_files

logging.basicConfig(level=logging.INFO, format="%(message)s")

# --- Folders ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# --- FAERS ZIP URLs ---
FAERS_URLS = {
    "Q1_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q1.zip",
    "Q2_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q2.zip"
}

# --- FAERS tables ---
FAERS_TABLES = ["DEMO25Q", "DRUG25Q", "REAC25Q"]#,
                #"OUTC25Q", "RPSR25Q", "INDI25Q", "THER25Q"]


def download_faers_data(raw_dir: Path):
    """
    Download FAERS ZIPs for 2025 Q1 and Q2, extract all relevant tables,
    and save as .txt files in raw_dir.
    Retries up to 3 times on network errors.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    # Skip download if all files exist
    existing_files = list(raw_dir.glob("*.txt"))
    if len(existing_files) >= 14:
        logging.info("All 14 FAERS TXT files already exist — skipping download")
        return existing_files

    downloaded_files = []

    for quarter, url in FAERS_URLS.items():
        logging.info(f"Downloading {quarter} ...")

        for attempt in range(3):
            try:
                # Stream download to handle large files
                with requests.get(url, stream=True, timeout=120, verify=False) as r:
                    r.raise_for_status()
                    buffer = io.BytesIO()
                    for chunk in r.iter_content(chunk_size=8192):
                        buffer.write(chunk)
                    buffer.seek(0)

                    # Open ZIP from memory
                    with zipfile.ZipFile(buffer) as z:
                        logging.info(f"ZIP contents: {z.namelist()}")
                        for f in z.namelist():
                            fname = f.split("/")[-1]
                            if fname.endswith(".txt") and any(fname.startswith(t) for t in FAERS_TABLES):
                                out_path = raw_dir / fname
                                if not out_path.exists():
                                    with z.open(f) as src, open(out_path, "wb") as out:
                                        out.write(src.read())
                                    logging.info(f"Downloaded '{fname}'")
                                else:
                                    logging.info(f"File '{fname}' already exists. Skipping.")
                                downloaded_files.append(out_path)
                break  # success, exit retry loop

            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    logging.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logging.error(f"Failed to download {quarter} after 3 attempts")
                    raise

    return downloaded_files