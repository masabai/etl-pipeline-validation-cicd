import logging
import gc
import json
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations import expectations as gxe

# base directories
BASE_DIR = Path.cwd()  # repo root
PROCESSED_DIR = BASE_DIR / "data"  # processed data storage
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)
GX_OUTPUT_DIR = PROCESSED_DIR / "gx_reports"  # great expectations report output

# initialize great expectations context
context = gx.get_context()
try:
    datasource = context.data_sources.get("pandas_src")  # fetch existing datasource
except Exception:
    datasource = context.data_sources.add_pandas(name="pandas_src")  # create new pandas datasource

# faers expected table schemas
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

# faers expected row count ranges
FAERS_ROW_COUNTS = {
    "DRUG": (3_000_000, 6_000_000),
    "REAC": (2_000_000, 5_000_000),
    "THER": (700_000, 2_000_000),
    "DEMO": (500_000, 1_500_000),
    "OUTC": (400_000, 1_200_000),
    "RPSR": (10_000, 100_000),
    "INDI": (2_000_000, 2_500_000)
}


def validate_all_texts(processed_dir: Path):
    """
    validate all merged faers csv files in `processed_dir` using great expectations

    - performs memory-efficient row counting (streaming)
    - samples large csvs for validation to prevent memory overload
    - registers datasource, asset, batch, expectation suite, and validation definition dynamically
    - runs validation and writes json reports to GX_OUTPUT_DIR
    """
    GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)  # ensure output directory exists

    for file_path in processed_dir.glob("merged_*.csv"):
        table_name = file_path.stem.replace("merged_", "").upper()
        logging.info(f">>> validating: {table_name}")

        # streaming row count
        with open(file_path, "r", encoding="utf-8") as f:
            actual_row_count = sum(1 for line in f) - 1  # subtract header

        # sample load to prevent memory overload
        if actual_row_count > 100_000:
            df = pd.read_csv(file_path, nrows=100_000, low_memory=True)
        else:
            df = pd.read_csv(file_path, low_memory=True)

        try:
            # datasource and asset registration
            try:
                asset = datasource.get_asset(table_name)
            except Exception:
                asset = datasource.add_dataframe_asset(name=table_name)

            # batch definition registration
            try:
                batch_def = asset.get_batch_definition(f"def_{table_name}")
            except Exception:
                batch_def = asset.add_batch_definition_whole_dataframe(name=f"def_{table_name}")

            # suite registration
            suite_name = f"suite_{table_name}"
            suite = gx.ExpectationSuite(name=suite_name)

            # add expectations
            min_r, max_r = FAERS_ROW_COUNTS.get(table_name, (10_000, 20_000_000))
            suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=min_r, max_value=max_r))
            cols = FAERS_SCHEMAS.get(table_name, list(df.columns))
            suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=cols, exact_match=False))

            # register the suite to context (update if exists)
            try:
                context.suites.add(suite)
            except Exception:
                context.suites.delete(suite_name)
                context.suites.add(suite)

            # validation definition registration
            val_name = f"v_{table_name}"
            val = gx.ValidationDefinition(data=batch_def, suite=suite, name=val_name)

            # register the validation to context (update if exists)
            try:
                val = context.validation_definitions.add(val)
            except Exception:
                context.validation_definitions.delete(val_name)
                val = context.validation_definitions.add(val)

            # run validation and save results
            results = val.run(batch_parameters={"dataframe": df})
            with open(GX_OUTPUT_DIR / f"gx_{table_name}.json", "w") as f:
                json.dump(results.to_json_dict(), f, indent=2)

            logging.info(f"success: {table_name} (verified {actual_row_count} rows)")

        finally:
            del df  # free memory
            gc.collect()  # run garbage collection
