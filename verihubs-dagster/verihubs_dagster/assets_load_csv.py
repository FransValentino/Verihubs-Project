import pandas as pd
import duckdb
from dagster import asset, ResourceDefinition
from dagster_duckdb import DuckDBResource
from pathlib import Path
import os
from verihubs_dagster import sql

# @asset()
# def csv_file_path():
#     return os.path.join(Path.cwd(), 'data\\Amazon Sale Report.csv')

@asset()
def load_csv(context):
    csv_file_path =  pd.read_csv(os.path.join(Path.cwd(), 'data\\Amazon Sale Report.csv'))
    # Load CSV data into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)
    
    # Establish DuckDB connection
    conn = duckdb.connect(":memory:")
    
    # Insert the data into the DuckDB table (if the table exists or create it)
    duckdb.sql("""
        CREATE TABLE IF NOT EXISTS sales_data AS SELECT * FROM df;
    """)
    conn.close()
    context.log.info(f"Successfully loaded data from {csv_file_path} into DuckDB")

# @asset()
# def load_to_duckdb_pipeline():
#     load_csv(csv_file_path())
