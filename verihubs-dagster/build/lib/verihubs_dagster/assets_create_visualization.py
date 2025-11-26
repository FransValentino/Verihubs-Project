from pathlib import Path
import os
import pandas as pd
import duckdb
from dagster import asset, MetadataValue
from . import sql
import seaborn as sns
import matplotlib.pyplot as plt

@asset(
    name="create_daily_visualization",
    description="generate chart that shows the number of orders daily separated by order status",
    group_name="visualization"
)
def create_daily_visualization(extract_csv: pd.DataFrame):
    df_daily = duckdb.sql("""
        SELECT Date(strptime(Date, '%m-%d-%y')) AS Date, Status, COUNT(Index) AS Orders
        FROM extract_csv
        GROUP BY Date, Status 
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
    
@asset(
    name="create_monthly_visualization",
    description="generate visual that shows the month with highest total revenue",
    group_name="visualization"
)
def create_monthly_visualization(extract_csv: pd.DataFrame):
    df_monthly = duckdb.sql("""
        SELECT Monthname(strptime(Date, '%m-%d-%y')) as Month, SUM(Qty * Amount) AS total_revenue
        FROM extract_csv
        GROUP BY Month
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
