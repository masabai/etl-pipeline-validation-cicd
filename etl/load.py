import pandas as pd
import logging
from snowflake.connector.pandas_tools import write_pandas

def load_csv_to_snowflake(csv_path, table: str, conn):
    """
    Memory-efficiently load a large CSV to Snowflake using chunking.
    Drops & recreates table on the first chunk, then appends.
    """
    db = conn.database
    schema = conn.schema
    cs = conn.cursor()
    
    # 1. Initialize schema and recreate target table structure
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")
    cs.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    
    # Read just the header to determine column names for creation
    header_df = pd.read_csv(csv_path, nrows=0)
    header_df.columns = [col.upper() for col in header_df.columns]
    columns_sql = ", ".join([f"{col} STRING" for col in header_df.columns])
    cs.execute(f"CREATE TABLE {schema}.{table} ({columns_sql})")
    cs.close()

    # 2. Process the file in chunks to prevent memory exhaustion (OOM)
    total_rows = 0
    chunk_size = 100000  # Adjust based on memory availability
    
    # TextFileReader iterator
    df_iterator = pd.read_csv(csv_path, chunksize=chunk_size, low_memory=False)
    
    for i, df in enumerate(df_iterator):
        df.columns = [col.upper() for col in df.columns]
        
        # Insert data using write_pandas
        success, nchunks, nrows, _ = write_pandas(
            conn=conn,
            df=df,
            table_name=table,
            database=db,
            schema=schema,
            auto_create_table=False,
            overwrite=False  # Always append after the initial CREATE TABLE
        )
        
        if success:
            total_rows += nrows
            logging.info(f"Chunk {i+1} loaded: {nrows} rows. Total: {total_rows}")
        else:
            logging.error(f"Failed to load chunk {i+1}")

    logging.info(f"Final load complete. Total rows inserted: {total_rows}")
    return total_rows
