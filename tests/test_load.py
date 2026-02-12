# tests/test_load.py
import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock
from etl import load  

# ---------------- Fixture ----------------
@pytest.fixture
def sample_csv(tmp_path):
    """Create a tiny CSV for testing load_csv_to_snowflake"""
    df = pd.DataFrame({
        "primaryid": ["1", "2"],
        "caseid": ["100", "101"],
        "drugname": ["ASPIRIN", "IBUPROFEN"]
    })
    csv_path = tmp_path / "DRUG_sample.csv"
    df.to_csv(csv_path, index=False)
    return csv_path

# ---------------- Test ----------------
def test_load_csv_to_snowflake(sample_csv):
    """
    Test load_csv_to_snowflake using mocked Snowflake connection.
    Validates chunking, table creation logic, and write_pandas calls.
    """
    # --- Mock the Snowflake connection ---
    mock_conn = MagicMock()
    mock_cursor = MagicMock()
    mock_conn.cursor.return_value = mock_cursor

    # --- Mock write_pandas to always succeed ---
    real_write_pandas = load.write_pandas
    load.write_pandas = MagicMock(return_value=(True, 1, 2, None))

    # --- Call the function ---
    total_rows = load.load_csv_to_snowflake(sample_csv, "DRUG", mock_conn)

    # --- Assertions ---
    # 1. write_pandas called at least once
    assert load.write_pandas.called

    # 2. Cursor executed CREATE and DROP table commands
    calls = [c[0][0] for c in mock_cursor.execute.call_args_list]
    assert any("CREATE TABLE" in call for call in calls)
    assert any("DROP TABLE" in call for call in calls)

    # 3. Total rows returned matches mocked write_pandas
    assert total_rows == 2

    # --- Restore original function ---
    load.write_pandas = real_write_pandas
