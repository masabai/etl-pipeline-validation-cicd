import pandas as pd
from pathlib import Path
import logging
from datetime import datetime

logging.basicConfig(level=logging.INFO, format="%(message)s")


    # Determine table prefixes (DEMO25Q1 → DEMO)
    table_prefixes = set(k[:-4] for k in dfs_dict.keys())

    for prefix in table_prefixes:
        table_dfs = [dfs_dict[k] for k in dfs_dict if k.startswith(prefix)]
        merged_df = pd.concat(table_dfs, ignore_index=True)

        upper_prefix = prefix.upper()
        if upper_prefix == "DEMO":
            merged_df = transform_demo(merged_df)
        elif upper_prefix == "DRUG":
            merged_df = transform_drug(merged_df)
        else:
            merged_df = transform_generic(merged_df, upper_prefix)

        out_file = output_dir / f"merged_{prefix.lower()}.csv"
        merged_df.to_csv(out_file, index=False)
        logging.info(f"{upper_prefix} staged → {len(merged_df)} rows")

    logging.info("All tables merged & transformed successfully.")
