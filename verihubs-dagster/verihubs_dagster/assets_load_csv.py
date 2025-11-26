from pathlib import Path
import os
import pandas as pd
import duckdb
from dagster import asset, MetadataValue
from . import sql

@asset(
    name="get_csv_path",
    description="get file path for required CSV file"
)
def get_csv_path(context):
    """Provide path to CSV file."""
    csv_file_path = os.path.join(Path.cwd(), 'data\\Amazon Sale Report.csv')
    context.log.info(csv_file_path)
    return csv_file_path

@asset(
    name="extract_csv",
    description="Load CSV data into dataframe",
    metadata={
        "file_type": MetadataValue.text("CSV")
    },
    compute_kind = "pandas"
)
def extract_csv(context, get_csv_path) -> pd.DataFrame:
    """Extract Data from CSV File."""
    df = pd.read_csv(get_csv_path)
    context.log.info(df.head())
    return df

@asset(
    name="insert_data_to_db",
    description="Load CSV data into dataframe",
    metadata={
        "database": MetadataValue.text("DuckDB"),
    }
)
def insert_data_to_db(context, extract_csv: pd.DataFrame):
    """Insert Dataframe data to DuckDB tables."""
    conn = sql.connect_db()
    sql.create_monthly_revenue(extract_csv)
    sql.create_daily_order_by_status(extract_csv)

    context.log.info(duckdb.sql("""
        SELECT * FROM monthly_revenue
    """).show())

    context.log.info(duckdb.sql("""
        SELECT * FROM daily_orders
    """).show())
    conn.close()