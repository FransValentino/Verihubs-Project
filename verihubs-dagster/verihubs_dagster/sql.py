import duckdb
import pandas as pd

def connect_db():
    conn = duckdb.connect("verihubs.db")
    return conn

def create_monthly_revenue(df):
    duckdb.sql("""
            CREATE TABLE IF NOT EXISTS monthly_revenue AS
                SELECT Monthname(strptime(Date, '%m-%d-%y')) AS Month, Category, SUM (Qty * Amount) AS Revenue
                FROM df
                GROUP BY Month, Category
                ORDER BY Month;
        """)

def create_daily_order_by_status(df):
    duckdb.sql("""
                CREATE TABLE IF NOT EXISTS daily_orders AS
                    SELECT Date(strptime(Date, '%m-%d-%y')) AS Date, Status, COUNT(Index) AS Orders
                    FROM df
                    GROUP BY Date, Status
                    ORDER BY Date;
            """)

def daily_orders_by_status_df(df):
    df_daily = duckdb.sql("""
                SELECT Date(strptime(Date, '%m-%d-%y')) AS Date, Status, COUNT(Index) AS Orders
                    FROM df
                    GROUP BY Date, Status 
                    ORDER BY Date ASC
            """).df()
    return df_daily

def monthly_highest_revenue(df):
    df_monthly = duckdb.sql("""
            SELECT Monthname(strptime(Date, '%m-%d-%y')) as Month, SUM(Qty * Amount) AS total_revenue
                FROM df
                GROUP BY Month
                ORDER BY total_revenue DESC
                LIMIT 1
            """).df()
    return df_monthly

