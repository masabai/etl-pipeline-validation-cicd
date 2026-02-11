import pytest
import io
import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock
from etl.extract import download_faers_data


@pytest.fixture
def temp_raw_dir(tmp_path):
    """Physical folder for FAERS raw files"""
    raw = tmp_path / "raw"
    raw.mkdir(parents=True, exist_ok=True)
    return raw


def _fake_zip():
    """Create a fake in-memory ZIP for testing"""
    buf = io.BytesIO()
    with zipfile.ZipFile(buf, "w") as z:
        z.writestr("ASCII/DEMO25Q1.txt", "fake data")
    buf.seek(0)
    return buf


@patch("requests.get")
def test_download_faers_data_success(mock_get, temp_raw_dir):
    """Test download + extraction works"""
    resp = MagicMock()
    resp.status_code = 200
    resp.iter_content.return_value = [_fake_zip().getvalue()]
    resp.__enter__.return_value = resp
    mock_get.return_value = resp

    files = download_faers_data(temp_raw_dir)

    assert len(files) > 0
    assert (temp_raw_dir / "DEMO25Q1.txt").exists()


def test_skip_if_already_exists(temp_raw_dir):
    """Test idempotency: skip if 14 files exist"""

    # ---- EASIEST WAY: create 14 real files on disk ----
    for i in range(14):
        f = temp_raw_dir / f"test_file_{i}.txt"
        with open(f, "wb") as fp:
            fp.write(b"x")  # dummy content

    # sanity check (optional)
    assert len(list(temp_raw_dir.iterdir())) == 14

    # now call extractor, should see files and skip
    files = download_faers_data(temp_raw_dir)

    assert len(files) == 14
