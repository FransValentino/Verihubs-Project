import seaborn as sns
import matplotlib.pyplot as plt
import duckdb
import test
import pandas as pd

# Connect to DuckDB and fetch data
conn = test.load_to_csv()
df_daily = duckdb.sql("""
    SELECT Date, Status, Orders 
    FROM daily_orders
    ORDER BY Date ASC
""").df()

# Pivoting table to create columns out of status
df_daily = df_daily.pivot(
    index = "Date",
    columns = "Status",
    values = "Orders"
)

# Cleaning inconsistent and null data
df_daily = df_daily.fillna(0)
df_daily['Pending'] = df_daily['Pending']+df_daily['Pending - Waiting for Pick Up']
df_daily['Shipped'] = df_daily['Shipped']+df_daily['Shipping']+df_daily['Shipped - Picked Up']+df_daily['Shipped - Out for Delivery']
df_daily['Returned to Seller'] = df_daily['Shipped - Returned to Seller'] + df_daily['Shipped - Returning to Seller']
df_daily.rename(columns = {'Shipped - Delivered to Buyer':'Delivered to Buyer'}, inplace=True)

df_daily = df_daily[['Pending', 'Shipped', 'Delivered to Buyer', 'Returned to Seller', 'Cancelled']]

# create plot
sns.set()
fig = plt.figure(figsize=(10, 7))
ax = fig.add_subplot(111)

df_daily['Pending'].plot(kind='barh', color='green', ax=ax, position=0)
df_daily['Shipped'].plot(kind='barh', color='blue', ax=ax, position=0)
df_daily['Delivered to Buyer'].plot(kind='barh', color='yellow', ax=ax, position=0)
df_daily['Returned to Seller'].plot(kind='barh', color='red', ax=ax, position=0)
df_daily['Cancelled'].plot(kind='barh', color='orange', ax=ax, position=0)

ax.grid(None)
ax.set_ylabel('Number of Orders')
plt.legend(loc="upper right")
plt.show()

conn.close()
