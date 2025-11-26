import pandas as pd
import duckdb
from pathlib import Path
import os

def load_to_csv():
    csv_file_path = os.path.join(Path.cwd(), 'data\\Amazon Sale Report.csv')

    # Load CSV data into a Pandas DataFrame
    df = pd.read_csv(csv_file_path)

    # Establish DuckDB connection
    conn = duckdb.connect(":memory:")

    #create monthly_revenue and daily_orders tables using data from Dataframe
    duckdb.sql("""
            CREATE TABLE IF NOT EXISTS monthly_revenue AS
                SELECT Monthname(strptime(Date, '%m-%d-%y')) AS Month, Category, SUM (Qty * Amount) AS Revenue
                FROM df
                GROUP BY Month, Category
                ORDER BY Month;
        """)

    duckdb.sql("""
            CREATE TABLE IF NOT EXISTS daily_orders AS
                SELECT Date(strptime(Date, '%m-%d-%y')) AS Date, Status, COUNT(Index) AS Orders
                FROM df
                GROUP BY Date, Status
                ORDER BY Date;
        """)

    duckdb.sql("""
        SELECT * FROM monthly_revenue
    """).show()

    duckdb.sql("""
        SELECT * FROM daily_orders
    """).show()

    return conn