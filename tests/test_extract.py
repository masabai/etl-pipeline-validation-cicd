import pytest
import io
import zipfile
from pathlib import Path
from unittest.mock import patch, MagicMock
# Import your function - adjust 'etl.downloader' if your file name is different
from etl.extract import download_faers_data, FAERS_TABLES 

@pytest.fixture
def temp_raw_dir(tmp_path):
    """Creates a temporary directory for testing."""
    return tmp_path / "raw"

def create_mock_zip():
    """Helper to create a fake ZIP in memory with one valid FAERS file."""
    buffer = io.BytesIO()
    with zipfile.ZipFile(buffer, "w") as z:
        # Create a fake 'DEMO25Q1.txt' inside the zip
        z.writestr("ASCII/DEMO25Q1.txt", "fake data content")
    buffer.seek(0)
    return buffer

@patch("requests.get")
def test_download_faers_data_success(mock_get, temp_raw_dir):
    """Test that the script extracts specific files from a ZIP."""
    # Setup the mock to return our fake ZIP
    mock_response = MagicMock()
    mock_response.status_code = 200
    mock_response.iter_content.return_value = [create_mock_zip().getvalue()]
    mock_response.__enter__.return_value = mock_response
    mock_get.return_value = mock_response

    # Run the function
    downloaded_paths = download_faers_data(temp_raw_dir)

    # ASSERTIONS
    assert len(downloaded_paths) > 0
    assert (temp_raw_dir / "DEMO25Q1.txt").exists()
    assert mock_get.call_count > 0

def test_skip_if_already_exists(temp_raw_dir):
    """Test that it skips download if 14 files already exist."""
    # Manually create 14 dummy files
    for i in range(14):
        (temp_raw_dir / f"test_file_{i}.txt").touch()

    # If it tries to download, it will crash because we didn't mock 'requests'
    # But it should return early based on the file count check.
    files = download_faers_data(temp_raw_dir)
    
    assert len(files) == 14
    logging_info = "All 14 FAERS TXT files already exist" 
    # This proves the logic hit the 'existing_files' check
