
import pandas as pd
import logging
from pathlib import Path
from snowflake.connector.pandas_tools import write_pandas

def load_csv_to_snowflake(csv_path, table: str, conn):
    """
    Load a CSV file to Snowflake using an existing connection.
    Drops & recreates table before insert.
    Returns number of rows inserted.
    """
    import pandas as pd
    import logging
    from snowflake.connector.pandas_tools import write_pandas

    df = pd.read_csv(csv_path, low_memory=False)
    df.columns = [col.upper() for col in df.columns]  # SF columns in upper
    logging.info(f"Loaded CSV {csv_path.name}, rows: {len(df)}")

    cs = conn.cursor()

    # Ensure schema exists (DB assumed already selected in conn)
    db = conn.database
    schema = conn.schema
    cs.execute(f"CREATE SCHEMA IF NOT EXISTS {db}.{schema}")

    # Drop & recreate table
    cs.execute(f"DROP TABLE IF EXISTS {schema}.{table}")
    columns_sql = ", ".join([f"{col} STRING" for col in df.columns])
    cs.execute(f"CREATE TABLE {schema}.{table} ({columns_sql})")

    # Insert data
    success, nchunks, nrows, _ = write_pandas(
        conn,
        df,
        table_name=table,
        database=db,
        schema=schema,
        auto_create_table=False,
        overwrite=True
    )

    logging.info(f"Data insert success: {success}, rows inserted: {nrows}")
    cs.close()
    return nrows