# test_extract.py
import pytest
from pathlib import Path
import etl.extract

@pytest.mark.parametrize("quarter,url", extract.FAERS_URLS.items())
def test_faers_extract(quarter, url, tmp_path):
    """
    Simple test: call extract.download_faers_data() and check that all expected tables
    are downloaded and non-empty. Uses tmp_path for CI isolation.
    """
    test_dir = tmp_path / "raw"
    test_dir.mkdir(parents=True, exist_ok=True)

    # Call the existing extract function
    downloaded_files = extract.download_faers_data(test_dir)

    # Check that all 7 expected tables are downloaded
    expected_tables = extract.FAERS_TABLES
    quarter_files = [f for f in downloaded_files if f.name.startswith(tuple(expected_tables))]
    assert len(quarter_files) == len(expected_tables), (
        f"{quarter}: expected {len(expected_tables)} tables, got {len(quarter_files)}"
    )

    # Check each file exists and is not empty
    for f in quarter_files:
        assert f.exists(), f"{f} not found"
        assert f.stat().st_size > 0, f"{f} is empty"
