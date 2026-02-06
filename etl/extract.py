"""
FAERS Data Downloader for 2025 Q1 and Q2

This script automates downloading and extracting FAERS ASCII ZIP files from FDA servers.

Features:
- Downloads FAERS ZIP archives for specified quarters.
- Extracts only relevant tables listed in FAERS_TABLES.
- Saves extracted .txt files to a structured raw data directory.
- Skips files that already exist to avoid redundant downloads.
- Retries network requests up to 3 times on failure.
- Logs progress and warnings for easy debugging.

Date: 2026-02-05
"""

import logging
from pathlib import Path
import requests
import zipfile
import io
import time

# ---------------- Logging Configuration ----------------
logging.basicConfig(level=logging.INFO, format="%(message)s")

# ---------------- Directory Setup ----------------
BASE_DIR = Path.cwd()  # Repository root
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- FAERS ZIP URLs ----------------
FAERS_URLS = {
    "Q1_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q1.zip",
    "Q2_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q2.zip"
}

# ---------------- FAERS Table Names ----------------
FAERS_TABLES = ["DEMO25Q", "DRUG25Q", "REAC25Q",
                "OUTC25Q", "RPSR25Q", "INDI25Q", "THER25Q"]


def download_faers_data(raw_dir: Path):
    """
    Download and extract FAERS ZIP files for 2025 Q1 and Q2.

    This function:
    - Downloads FAERS ZIP archives from FDA servers.
    - Extracts relevant tables listed in FAERS_TABLES.
    - Saves extracted .txt files to `raw_dir`.
    - Skips files that already exist.
    - Retries up to 3 times on network errors.

    Args:
        raw_dir (Path): Directory where raw FAERS .txt files will be saved.

    Returns:
        list[Path]: Paths of downloaded or existing FAERS .txt files.

    Raises:
        requests.exceptions.RequestException: If all retries fail for a given quarter.
    """
    raw_dir.mkdir(parents=True, exist_ok=True)

    # ---------------- Skip Download if Files Already Exist ----------------
    existing_files = list(raw_dir.glob("*.txt"))
    if len(existing_files) >= 14:
        logging.info("All 14 FAERS TXT files already exist — skipping download")
        return existing_files

    downloaded_files = []

    # ---------------- Loop Over Quarters ----------------
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

                    # Extract ZIP in memory
                    with zipfile.ZipFile(buffer) as z:
                        logging.info(f"ZIP contents: {z.namelist()}")
                        for f in z.namelist():
                            fname = f.split("/")[-1]
                            # Only extract relevant FAERS tables
                            if fname.endswith(".txt") and any(fname.startswith(t) for t in FAERS_TABLES):
                                out_path = raw_dir / fname
                                if not out_path.exists():
                                    with z.open(f) as src, open(out_path, "wb") as out:
                                        out.write(src.read())
                                    logging.info(f"Downloaded '{fname}'")
                                else:
                                    logging.info(f"File '{fname}' already exists. Skipping.")
                                downloaded_files.append(out_path)
                break  # Success, exit retry loop

            except requests.exceptions.RequestException as e:
                logging.warning(f"Attempt {attempt+1} failed: {e}")
                if attempt < 2:
                    logging.info("Retrying in 5 seconds...")
                    time.sleep(5)
                else:
                    logging.error(f"Failed to download {quarter} after 3 attempts")
                    raise

    return downloaded_files
