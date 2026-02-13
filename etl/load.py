"""
Load Large CSV into Snowflake with Chunked Memory-Efficient Inserts

This script loads a large CSV into a Snowflake table using chunking
and the Snowflake `write_pandas` utility.

Features:
- Drops and recreates the target table on the first chunk.
- Appends subsequent chunks to avoid memory issues.
- Uses write_pandas for efficient bulk insert.
- Logs progress per chunk and total rows loaded.
- Column names are uppercased for Snowflake conventions.

Date: 2026-02-05
"""

import pandas as pd
import logging
from snowflake.connector.pandas_tools import write_pandas


def load_csv_to_snowflake(csv_path, table: str, conn):
    """
    Memory-efficiently load a large CSV into a Snowflake table using chunking.

    Args:
        csv_path: Path to the CSV file to be loaded.
        table: Target table name in Snowflake.
        conn: Active Snowflake connection object.

    Returns:
        int: Total number of rows successfully inserted.
    """
    db = conn.database
    schema = conn.schema
    cs = conn.cursor()

    # Initialize schema and recreate target table structure
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
    cs.execute(f"DROP TABLE IF EXISTS {schema}.{table}")

    # Read header only to define table columns
    header_df = pd.read_csv(csv_path, nrows=0)
    header_df.columns = [col.upper() for col in header_df.columns]
    columns_sql = ", ".join([f"{col} STRING" for col in header_df.columns])
    cs.execute(f"CREATE TABLE {schema}.{table} ({columns_sql})")
    cs.close()

    # Load CSV in chunks to prevent memory issues
    total_rows = 0
    chunk_size = 100_000
    df_iterator = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)

    for i, df in enumerate(df_iterator):
        df.columns = [col.upper() for col in df.columns]
        
        # Write DataFrame to Snowflake; ignore fourth return value (metadata) 
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            database=db,
            schema=schema,
            auto_create_table=False,
            overwrite=False
        )

        if success:
            total_rows += nrows
            logging.info(f"Chunk {i + 1} loaded: {nrows} rows. Total: {total_rows}")
        else:
            logging.error(f"Failed to load chunk {i + 1}")

    logging.info(f"Final load complete. Total rows inserted: {total_rows}")
    return total_rows
