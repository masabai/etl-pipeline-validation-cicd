# validation/extract_gx.py

import logging
from pathlib import Path
import pandas as pd
import json
import gc
import great_expectations as gx
from great_expectations import expectations as gxe

# ---------------- Paths ----------------
PROJECT_ROOT = Path(__file__).resolve().parent.parent
GX_OUTPUT_DIR = PROJECT_ROOT / "data" / "validation"
GX_OUTPUT_DIR.mkdir(parents=True, exist_ok=True)

# ---------------- GX Global Setup ----------------
# Moving these here prevents the "closure" warnings and memory exhaustion
context = gx.get_context()

try:
    datasource = context.data_sources.get("pandas_src")
except Exception:
    datasource = context.data_sources.add_pandas(name="pandas_src")

# ---------------- FDA FAERS Contracts ----------------
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

FAERS_ROW_COUNTS = {
    "DRUG": (3_000_000, 6_000_000),
    "REAC": (2_000_000, 5_000_000),
    "THER": (700_000, 2_000_000),
    "DEMO": (500_000, 1_500_000),
    "OUTC": (400_000, 1_200_000),
    "RPSR": (10_000, 100_000),
    "INDI": (2_000_000, 2_500_000)
}

# ---------------- Functions ----------------

def run_data_validation(df: pd.DataFrame, batch_name: str):
    """Run GX 1.0 validation for a single DataFrame batch."""
    
    # 1. Get or Create Asset
    try:
        asset = datasource.get_asset(batch_name)
    except Exception:
        asset = datasource.add_dataframe_asset(name=batch_name)
    
    # 2. Get or Create Batch Definition
    batch_def_name = f"def_{batch_name}"
    try:
        batch_def = asset.get_batch_definition(batch_def_name)
    except Exception:
        batch_def = asset.add_batch_definition_whole_dataframe(name=batch_def_name)

    # 3. Build Suite
    suite = gx.ExpectationSuite(name=f"suite_{batch_name}")
    expected_columns = FAERS_SCHEMAS.get(batch_name, list(df.columns))

    # Volume check
    min_rows, max_rows = FAERS_ROW_COUNTS.get(batch_name, (10_000, 20_000_000))
    suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=min_rows, max_value=max_rows))

    # Schema drift
    suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=expected_columns, exact_match=False))

    # Key integrity
    suite.add_expectation(gxe.ExpectColumnValuesToNotBeNull(column="primaryid"))
    suite.add_expectation(gxe.ExpectColumnValuesToBeUnique(column="row_num"))

    # Domain rules
    if "age" in df.columns:
        suite.add_expectation(gxe.ExpectColumnValuesToBeBetween(column="age", min_value=0, max_value=130, mostly=0.98))
    if "sex" in df.columns:
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="sex", value_set=["M", "F", "UNK"]))
    if "role_cod" in df.columns:
        suite.add_expectation(gxe.ExpectColumnValuesToBeInSet(column="role_cod", value_set=["PS", "SS", "C", "I"]))

    # 4. Execute Validation
    validation = gx.ValidationDefinition(data=batch_def, suite=suite, name=f"val_{batch_name}")
    results = validation.run(batch_parameters={"dataframe": df})

    return results


def validate_all_texts(processed_dir: Path):
    """Loop through processed CSVs, validate, and clear memory."""
    for file_path in processed_dir.glob("merged_*.csv"):
        table_name = file_path.stem.replace("merged_", "").upper()
        logging.info(f"Validating {table_name}")

        # Loading as string to handle messy FAERS raw data
        df = pd.read_csv(file_path, dtype=str)
        
        try:
            results = run_data_validation(df, batch_name=table_name)
            
            output_file = GX_OUTPUT_DIR / f"gx_{table_name}.json"
            with open(output_file, "w") as f:
                json.dump(results.to_json_dict(), f, indent=2)
            
            logging.info(f"Saved validation: {output_file}")
        except Exception as e:
            logging.error(f"Validation failed for {table_name}: {e}")
        finally:
            # CRITICAL: Manually clear memory to prevent pipeline crashes
            del df
            gc.collect()
