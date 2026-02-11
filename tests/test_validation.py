import pytest
import pandas as pd
from pathlib import Path
from unittest.mock import MagicMock, patch

# ---------------- Fixture ----------------
@pytest.fixture
def processed_dir(tmp_path):
    dir_path = tmp_path / "processed"
    dir_path.mkdir(parents=True, exist_ok=True)
    for t in ["DEMO", "DRUG", "REAC"]:
        df = pd.DataFrame({"primaryid": ["1"], "caseid": ["100"]})
        df.to_csv(dir_path / f"merged_{t}.csv", index=False)
    return dir_path

# ---------------- Test ----------------
@patch("validation.extract_gx.gx.get_context")
def test_validate_all_texts(mock_gx_context, processed_dir):
    # 1️⃣ Mock GE context and validation_definitions
    mock_context = MagicMock()
    mock_gx_context.return_value = mock_context

    mock_val_defs = MagicMock()
    mock_context.validation_definitions = mock_val_defs

    # Add/delete do nothing real
    mock_val_defs.add.side_effect = lambda val: val
    mock_val_defs.delete.side_effect = lambda name: None

    # 2️⃣ Mock data sources and assets
    mock_ds = MagicMock()
    mock_context.data_sources.get.return_value = mock_ds
    mock_context.data_sources.add_pandas.return_value = mock_ds

    mock_asset = MagicMock()
    mock_ds.get_asset.side_effect = Exception("not found")
    mock_ds.add_dataframe_asset.return_value = mock_asset
    mock_asset.get_batch_definition.side_effect = Exception("not found")
    mock_asset.add_batch_definition_whole_dataframe.return_value = "batch_def"

    # 3️⃣ Patch ValidationDefinition to return a dummy with .run()
    with patch("validation.extract_gx.gx.ValidationDefinition") as mock_val_class:
        mock_val_instance = MagicMock()
        # .run() returns an object that has .to_json_dict()
        mock_run_result = MagicMock()
        mock_run_result.to_json_dict.return_value = {"success": True}
        mock_val_instance.run.return_value = mock_run_result
        mock_val_class.return_value = mock_val_instance

        # 4️⃣ Import inside patch so patch is active
        from validation import extract_gx
        extract_gx.validate_all_texts(processed_dir)

    # 5️⃣ Assertions
    assert mock_val_instance.run.call_count == 3  # DEMO, DRUG, REAC
    assert mock_val_defs.delete.called
