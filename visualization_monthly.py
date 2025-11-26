import matplotlib.pyplot as plt
import duckdb
import test

# Connect to DuckDB and fetch data
conn = test.load_to_csv()

df_monthly = duckdb.sql("""
    SELECT month, SUM(revenue) AS total_revenue
    FROM monthly_revenue
    GROUP BY month
    ORDER BY total_revenue DESC
    LIMIT 1
""").df()

# Plot the data
plt.figure(figsize=(10, 6))
plt.text(x = 0.5,
         y = 0.8,
         s="Highest Monthly Revenue",
         fontdict = {'fontsize': 25},
         horizontalalignment = 'center')
plt.text(x = 0.5,
         y = 0.5,
         s=df_monthly['Month'].loc[df_monthly.index[0]],
         fontdict = {'fontsize': 25},
         horizontalalignment = 'center')
plt.text(x = 0.5,
         y = 0.3,
         s=df_monthly['total_revenue'].loc[df_monthly.index[0]],
         fontdict = {'fontsize': 25},
         horizontalalignment = 'center')
plt.xticks([])
plt.yticks([])
plt.show()
conn.close()
