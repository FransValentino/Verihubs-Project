import matplotlib.pyplot as plt
import seaborn as sns
import duckdb

# Connect to DuckDB and fetch data
conn = duckdb.connect(":memory:")
query = """
    SELECT * FROM sales_data;
"""
df_orders = conn.execute(query).fetchdf()

# Plot the data
plt.figure(figsize=(12, 6))
sns.lineplot(data=df_orders, x='day', y='num_orders', hue='status', marker='o')
plt.title('Daily Orders by Order Status')
plt.xticks(rotation=45)
plt.xlabel('Day')
plt.ylabel('Number of Orders')
plt.tight_layout()
plt.show()
conn.close()
