import logging
from pathlib import Path
import pandas as pd
import json
import great_expectations as gx
from great_expectations.expectations import *

# Paths (Codespaces + GitHub Actions)
PROJECT_ROOT = Path(__file__).resolve().parent.parent
RAW_DIR = PROJECT_ROOT / "data" / "raw"
GX_OUTPUT_DIR = PROJECT_ROOT / "data" / "validation"
GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

"""
Validates FDA FAERS raw TXT files using Great Expectations.
Applies volume, schema, key integrity, and domain checks.
"""

# Expected FAERS schemas (stable join keys)
FAERS_SCHEMAS = {
    "DEMO": ["primaryid", "caseid", "caseversion", "i_f_code", "event_dt",
             "age", "age_cod", "sex", "wt", "reporter_country", "row_num"],
    "DRUG": ["primaryid", "drug_seq", "role_cod", "drugname",
             "prod_ai", "row_num"],
    "REAC": ["primaryid", "pt", "row_num"],
    "OUTC": ["primaryid", "outc_cod", "row_num"],
    "THER": ["primaryid", "dsg_drug_seq", "start_dt", "end_dt", "row_num"],
    "RPSR": ["primaryid", "rpsr_cod", "row_num"],
    "INDI": ["primaryid", "indi_seq", "indi_name", "row_num"],
}

# Historical FAERS row count contracts
FAERS_ROW_COUNTS = {
    "DRUG": (3_000_000, 6_000_000),
    "REAC": (2_000_000, 5_000_000),
    "THER": (700_000, 2_000_000),
    "DEMO": (500_000, 1_500_000),
    "OUTC": (400_000, 1_200_000),
    "RPSR": (10_000, 100_000),
    "INDI": (2_000_000, 2_500_000)
}

def run_data_validation(df: pd.DataFrame, batch_name: str):
    context = gx.get_context()

    datasource = context.data_sources.add_pandas(name="pandas_src")
    asset = datasource.add_dataframe_asset(name=batch_name)
    batch_def = asset.add_batch_definition_whole_dataframe(name=batch_name)

    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    context.suites.add(suite)

    expected_columns = FAERS_SCHEMAS.get(batch_name, list(df.columns))

    # ---------- VOLUME CONTRACT ----------

    min_rows, max_rows = FAERS_ROW_COUNTS.get(batch_name, (10_000, 20_000_000))
    suite.add_expectation(
        ExpectTableRowCountToBeBetween(
            min_value=min_rows,
            max_value=max_rows
        )
    )

    # ---------- SCHEMA DRIFT ----------

    suite.add_expectation(
        ExpectTableColumnsToMatchSet(
            column_set=expected_columns,
            exact_match=False  # FDA may add columns
        )
    )

    # ---------- KEY INTEGRITY ----------

    suite.add_expectation(
        ExpectColumnValuesToNotBeNull(column="primaryid")
    )

    suite.add_expectation(
        ExpectColumnValuesToBeUnique(column="row_num")
    )

    # ---------- DOMAIN RULES ----------

    if "age" in df.columns:
        suite.add_expectation(
            ExpectColumnValuesToBeBetween(
                column="age", min_value=0, max_value=130, mostly=0.98
            )
        )

    if "sex" in df.columns:
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="sex", value_set=["M", "F", "UNK"]
            )
        )

    if "role_cod" in df.columns:
        suite.add_expectation(
            ExpectColumnValuesToBeInSet(
                column="role_cod", value_set=["PS", "SS", "C", "I"]
            )
        )

    # ---------- RUN VALIDATION ----------

    validation = gx.ValidationDefinition(
        data=batch_def,
        suite=suite,
        name=f"validation_{batch_name}"
    )

    results = validation.run(batch_parameters={"dataframe": df})
    return results


def validate_all_texts():
    for file_path in RAW_DIR.glob("*.txt"):
        table_name = file_path.stem.split("25")[0].upper()  # DEMO25Q1 → DEMO
        logging.info(f"Validating {table_name}")

        df = pd.read_csv(file_path, sep="$", dtype=str)

        results = run_data_validation(df, batch_name=table_name)

        output_file = GX_OUTPUT_DIR / f"gx_{table_name}.json"
        with open(output_file, "w") as f:
            json.dump(results.to_json_dict(), f, indent=2)

        logging.info(f"Saved validation: {output_file}")
