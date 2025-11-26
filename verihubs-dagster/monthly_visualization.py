import matplotlib.pyplot as plt
import seaborn as sns
import duckdb

# Connect to DuckDB and fetch data
conn = duckdb.connect(":memory:")
query = """
    SELECT * FROM monthly_revenue_by_category;
"""
df_revenue = conn.execute(query).fetchdf()

# Plot the data
plt.figure(figsize=(10, 6))
sns.lineplot(data=df_revenue, x='month', y='total_revenue', hue='product_category', marker='o')
plt.title('Monthly Total Revenue by Product Category')
plt.xticks(rotation=45)
plt.xlabel('Month')
plt.ylabel('Total Revenue')
plt.tight_layout()
plt.show()
conn.close()
