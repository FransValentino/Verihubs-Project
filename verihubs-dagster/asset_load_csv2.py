import pandas as pd
import duckdb
from dagster import asset, io_manager, MetadataValue

# Define an IO Manager for DuckDB (optional, but good for managing DuckDB resources)
class DuckDBIOManager:
    def __init__(self, db_path: str):
        self.db_path = db_path

    def handle_output(self, context, obj):
        # Here, you could define how to store output (for example, inserting into a DuckDB table)
        pass

    def load_input(self, context):
        # Here, you could define how to load input data (from DuckDB, etc.)
        pass

# Define the Dagster asset that loads CSV data and inserts it into DuckDB
@asset(
    name="load_csv_to_duckdb",
    description="Load CSV data into a DuckDB table",
    metadata={
        "file_type": MetadataValue.text("CSV"),
        "database": MetadataValue.text("DuckDB"),
    },
)
def load_csv_to_duckdb(context, csv_file_path: str, db_path: str):
    """
    This asset reads a CSV file and inserts its content into a DuckDB table.
    """
    # Step 1: Load CSV data into a Pandas DataFrame
    context.log.info(f"Loading CSV file from {csv_file_path}...")
    df = pd.read_csv(csv_file_path)

    # Step 2: Create a DuckDB connection
    conn = duckdb.connect(db_path)

    # Optional: Drop the table if it already exists (or you can handle the table schema differently)
    conn.execute("DROP TABLE IF EXISTS sales_data")

    # Step 3: Insert the DataFrame into the DuckDB table
    # Create the table if it doesn't exist and insert data
    conn.execute("""
        CREATE TABLE sales_data AS
        SELECT * FROM df
    """)

    context.log.info(f"Successfully loaded {len(df)} records into DuckDB table 'sales_data'.")

    # Step 4: Close the DuckDB connection
    conn.close()

    # Return metadata if needed (optional)
    return MetadataValue.text(f"Inserted {len(df)} rows into DuckDB")

