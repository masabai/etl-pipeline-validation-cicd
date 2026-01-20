import logging
import gc
import json
from pathlib import Path
import pandas as pd
import great_expectations as gx
from great_expectations import expectations as gxe


BASE_DIR = Path.cwd()  # repo root
RAW_DIR = BASE_DIR / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)
GX_OUTPUT_DIR = RAW_DIR/"gx_reports"
GX_OUTPUT_DIR

# Initialize GX ONCE at top level
context = gx.get_context()
try:
    datasource = context.data_sources.get("pandas_src")
except Exception:
    datasource = context.data_sources.add_pandas(name="pandas_src")

# ... (Keep your FAERS_SCHEMAS and FAERS_ROW_COUNTS here) ...
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

def validate_all_texts(processed_dir: Path):
    """Memory-first orchestration."""
    for file_path in processed_dir.glob("merged_*.csv"):
        table_name = file_path.stem.replace("merged_", "").upper()
        logging.info(f">>> Processing: {table_name}")

        # LOAD ONLY ONE FILE AT A TIME
        # use low_memory=True to reduce footprint
        df = pd.read_csv(file_path, dtype=str, low_memory=True)
        
        try:
            # Re-use the existing global context and datasource
            try:
                asset = datasource.get_asset(table_name)
            except Exception:
                asset = datasource.add_dataframe_asset(name=table_name)
            
            batch_def = asset.add_batch_definition_whole_dataframe(name=f"def_{table_name}")
            suite = gx.ExpectationSuite(name=f"suite_{table_name}")

            # Volume check from your FAERS_ROW_COUNTS
            min_r, max_r = FAERS_ROW_COUNTS.get(table_name, (10_000, 20_000_000))
            suite.add_expectation(gxe.ExpectTableRowCountToBeBetween(min_value=min_r, max_value=max_r))

            # Column check from your FAERS_SCHEMAS
            cols = FAERS_SCHEMAS.get(table_name, list(df.columns))
            suite.add_expectation(gxe.ExpectTableColumnsToMatchSet(column_set=cols, exact_match=False))

            # Run validation
            val = gx.ValidationDefinition(data=batch_def, suite=suite, name=f"v_{table_name}")
            results = val.run(batch_parameters={"dataframe": df})

            with open(GX_OUTPUT_DIR / f"gx_{table_name}.json", "w") as f:
                json.dump(results.to_json_dict(), f, indent=2)
            
            logging.info(f"Success: {table_name}")

        finally:
            # FORCE PURGE"
            del df
            gc.collect()
            logging.info(f"RAM Freed for {table_name}")
