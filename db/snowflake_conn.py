"""
Snowflake Connection Helper for ETL Pipelines

This module provides a single function to establish a Snowflake connection
for ETL workflows, loading FAERS datasets, or running dbt transformations.

Features:
- Pulls connection credentials from environment variables:
    - SNOW_USER
    - SNOW_PASSWORD
    - SNOW_ACCOUNT
    - SNOW_WAREHOUSE
- Connects to database 'ETL_TESTING', schema 'FDA', with role 'ETL_PIPELINE'.
- Sets insecure_mode=True to allow connections in Codespaces without full SSL verification.

Returns:
    snowflake.connector.SnowflakeConnection: Active Snowflake connection object.

Date: 2026-02-05
"""

import snowflake.connector
import os

def get_snowflake_connection():
    """
    Establish a connection to the Snowflake data warehouse for ETL pipelines.

    Connection details are pulled from environment variables:
        - SNOW_USER
        - SNOW_PASSWORD
        - SNOW_ACCOUNT
        - SNOW_WAREHOUSE

    Returns:
        snowflake.connector.SnowflakeConnection: Active Snowflake connection object.

    Notes:
        - Uses role 'ETL_PIPELINE' and connects to database 'ETL_TESTING', schema 'FDA'.
        - insecure_mode=True is set to allow connections in Codespaces without full SSL verification.
    """
    return snowflake.connector.connect(
        user=os.environ["SNOW_USER"],
        password=os.environ["SNOW_PASSWORD"],
        account=os.environ["SNOW_ACCOUNT"],
        warehouse=os.environ["SNOW_WAREHOUSE"],
        role="ETL_PIPELINE",
        database="ETL_TESTING",
        schema="FDA",
        insecure_mode=True  # allows the connection to proceed even if the security check fails in Codespace
    )
