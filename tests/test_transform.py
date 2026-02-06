# test_transform.py
import pytest
from pathlib import Path
import pandas as pd
import etl.transform


@pytest.fixture
def sample_demo_df(tmp_path):
    """
    Create a small sample DEMO dataframe for testing transform_demo
    """
    df = pd.DataFrame({
        "primaryid": ["1", "2", None],
        "caseid": ["100", None, None],
        "age": ["25", "30", "NaN"],
        "sex": ["m", "F", None],
        "wt": ["70", "80", "NaN"],
        "some_col": ["A", "B", None]
    })
    file_path = tmp_path / "DEMO_sample.txt"
    df.to_csv(file_path, sep="$", index=False)
    return file_path


@pytest.fixture
def sample_drug_df(tmp_path):
    """
    Small sample DRUG dataframe
    """
    df = pd.DataFrame({
        "primaryid": ["1", "2"],
        "caseid": ["100", "101"],
        "drugname": ["aspirin", "ibuprofen"],
        "role_cod": ["ps", "pt"]
    })
    file_path = tmp_path / "DRUG_sample.txt"
    df.to_csv(file_path, sep="$", index=False)
    return file_path


def test_transform_demo(sample_demo_df, tmp_path):
    # Load dataframe
    dfs = transform.load_txt_files(sample_demo_df.parent)
    df = dfs[sample_demo_df.stem]

    # Apply transform
    transformed = transform.transform_demo(df)

    # Check load_ts added
    assert "load_ts" in transformed.columns

    # Check age/wt converted to numeric
    assert pd.api.types.is_numeric_dtype(transformed["age"])
    assert pd.api.types.is_numeric_dtype(transformed["wt"])

    # Check sex normalized
    assert set(transformed["sex"].unique()) <= {"M", "F", "UNKNOWN"}

    # Check no rows with primaryid & caseid null
    null_rows = transformed[transformed["primaryid"].isnull() & transformed["caseid"].isnull()]
    assert null_rows.empty


def test_transform_drug(sample_drug_df, tmp_path):
    dfs = transform.load_txt_files(sample_drug_df.parent)
    df = dfs[sample_drug_df.stem]

    transformed = transform.transform_drug(df)

    # Check load_ts added
    assert "load_ts" in transformed.columns

    # Check drugname/role_cod normalized to uppercase
    assert all(transformed["drugname"] == transformed["drugname"].str.upper())
    assert all(transformed["role_cod"] == transformed["role_cod"].str.upper())


def test_merge_and_transform_one_by_one(tmp_path):
    """
    Smoke test for merge_and_transform_one_by_one function.
    Uses a tiny dataset to avoid heavy RAM usage.
    """
    raw_dir = tmp_path / "raw"
    output_dir = tmp_path / "output"
    raw_dir.mkdir()
    output_dir.mkdir()

    # Create tiny DEMO & DRUG files
    demo_df = pd.DataFrame({"primaryid": ["1"], "caseid": ["100"], "age": ["25"], "sex": ["M"]})
    demo_df.to_csv(raw_dir / "DEMO_sample.txt", sep="$", index=False)
    drug_df = pd.DataFrame({"primaryid": ["1"], "caseid": ["100"], "drugname": ["aspirin"], "role_cod": ["ps"]})
    drug_df.to_csv(raw_dir / "DRUG_sample.txt", sep="$", index=False)

    # Run merge_and_transform
    transform.merge_and_transform_one_by_one(raw_dir, output_dir)

    # Check output files exist
    output_files = list(output_dir.glob("merged_*.csv"))
    assert len(output_files) >= 2  # DEMO + DRUG

    for f in output_files:
        assert f.stat().st_size > 0  # not empty
