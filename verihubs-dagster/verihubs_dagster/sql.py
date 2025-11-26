import duckdb

def create_monthly_revenue(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS monthly_revenue_by_category AS
        SELECT
            strftime('%Y-%m', order_date) AS month,
            product_category,
            SUM(revenue) AS total_revenue
        FROM sales_data
        GROUP BY month, product_category;
    """)
    conn.close()

def create_daily_order_by_status(conn):
    conn.execute("""
        CREATE TABLE IF NOT EXISTS daily_orders_by_status AS
        SELECT
            DATE(order_date) AS day,
            status,
            COUNT(*) AS num_orders
        FROM sales_data
        GROUP BY day, status;
    """)
    conn.close()
