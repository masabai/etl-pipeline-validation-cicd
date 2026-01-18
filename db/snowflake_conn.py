import snowflake.connector
import os

def get_snowflake_connection():
    return snowflake.connector.connect(
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        account=os.environ["SNOW_ACCOUNT"],
        warehouse=os.environ["SNOW_WAREHOUSE"],
        role="ETL_PIPELINE",
        database="ETL_TESTING",
        schema="FDA",
        insecure_mode=True #allows the connection to proceed even if the security fail in Codespace
    )
    
    return snowflake.connector.connect(
        user=user,
        password=password,
        account=account,
        warehouse=warehouse,
        role=role,
        database=database,
        schema=schema
    )

