import logging
from pathlib import Path
import requests
import zipfile
import io
import time

logging.basicConfig(level=logging.INFO, format="%(message)s")

# --- Paths ---
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

logging.info(f"PROJECT_ROOT resolved to: {PROJECT_ROOT}")
logging.info(f"RAW_DIR resolved to: {RAW_DIR}")

# --- FAERS ZIP URLs ---
FAERS_URLS = {
    "Q1_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q1.zip",
    "Q2_2025": "https://fis.fda.gov/content/Exports/FAERS_ASCII_2025Q2.zip"
}

# --- FAERS tables ---
FAERS_TABLES = [
    "DEMO25Q", "DRUG25Q", "REAC25Q",
    "OUTC25Q", "RPSR25Q", "INDI25Q", "THER25Q"
]

def download_faers_data(raw_dir: Path = RAW_DIR):
    raw_dir.mkdir(parents=True, exist_ok=True)

    existing_files = list(raw_dir.glob("*.txt"))
    if len(existing_files) >= 14:
        logging.info("All 14 FAERS TXT files already exist — skipping download")
        return existing_files

    headers = {"User-Agent": "etl-pipeline-testing/1.0"}
    downloaded_files = []

    for quarter, url in FAERS_URLS.items():
        logging.info(f"Downloading {quarter} ...")

        for attempt in range(3):
            try:
                with requests.get(
                    url,
                    stream=True,
                    timeout=(10, 120),
                    headers=headers
                ) as r:
                    r.raise_for_status()
                    buffer = io.BytesIO()
                    for chunk in r.iter_content(chunk_size=8192):
                        buffer.write(chunk)
                    buffer.seek(0)

                    with zipfile.ZipFile(buffer) as z:
                        for f in z.namelist():
                            fname = f.split("/")[-1]
                            if fname.endswith(".txt") and any(fname.startswith(t) for t in FAERS_TABLES):
                                out_path = raw_dir / fname
                                if not out_path.exists():
                                    with z.open(f) as src, open(out_path, "wb") as out:
                                        out.write(src.read())
                                    logging.info(f"Downloaded '{fname}'")
                                downloaded_files.append(out_path)
                break

            except requests.RequestException as e:
                logging.warning(f"Attempt {attempt + 1} failed: {e}")
                if attempt < 2:
                    time.sleep(5)
                else:
                    raise

    return downloaded_files
