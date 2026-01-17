# Note: Demo script 
import logging
from pathlib import Path
import pandas as pd
import json
import great_expectations as gx
from great_expectations.expectations import (
    ExpectTableRowCountToBeBetween,
    ExpectTableColumnCountToEqual,
    ExpectColumnToExist,
)

# Paths
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
GX_OUTPUT_DIR = PROJECT_ROOT / "data" / "validation"
GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
print("Testing PR")

"""
Module: extract_gx

This module validates raw TXT files using Great Expectations.
Each TXT file is loaded as a pandas DataFrame and validated for:
    - Row count within a specified range
    - Exact column count
    - Presence of all columns

Validation results are saved as JSON files in the GX_OUTPUT_DIR, 
overwriting previous results for each table.
"""
def run_data_validation(df, batch_name="my_batch"):
    """
    Run Great Expectations validation on a single DataFrame.

    Parameters:
        df (pd.DataFrame): The DataFrame to validate.
        batch_name (str): Logical name for the batch/dataset.

    Returns:
        results: The validation results object from Great Expectations.
    """
    context = gx.get_context()
    data_source = context.data_sources.add_pandas(name="my_pandas_datasource")
    data_asset = data_source.add_dataframe_asset(name=batch_name)
    batch_definition = data_asset.add_batch_definition_whole_dataframe(name=batch_name)
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    context.suites.add(suite)

    # Table expectations
    suite.add_expectation(ExpectTableRowCountToBeBetween(min_value=50000, max_value=200000))

    # Column expectations
    expected_columns = list(df.columns)

    # Check exact column count
    suite.add_expectation(ExpectTableColumnCountToEqual(value=len(expected_columns)))

    # Check each required column exists
    for col in expected_columns:
        suite.add_expectation(ExpectColumnToExist(column=col))

    validation_definition = gx.ValidationDefinition(
        data=batch_definition,
        suite=suite,
        name=f"validation_{batch_name}"
    )
    results = validation_definition.run(batch_parameters={"dataframe": df})
    return results


def validate_all_texts():
    """
    Loop through all TXT files in RAW_DIR, validate each using Great Expectations,
    and save the validation results as JSON in GX_OUTPUT_DIR.

    Overwrites existing JSON files for each table.
    """
    for file_path in RAW_DIR.glob("*.txt"):
        table_name = file_path.stem.upper()
        logging.info(f"\nValidating {table_name}")
        df = pd.read_csv(file_path, sep="$", dtype=str)

        validation_results = run_data_validation(df, batch_name=table_name)
        output_file = GX_OUTPUT_DIR / f"gx_{table_name}.json"

        # Overwrite previous validation results
        with open(output_file, "w") as f:
            json.dump(validation_results.to_json_dict(), f, indent=2)

        logging.info(f"Validation results saved: {output_file}")