
# Import & Configs
import sqlite3
import pandas as pd

# Temporarily hard coded, can be replaced by using config file 
DB_FILE = "../S30 ETL Assignment.db"
SOLUTION_CSV = "Solution.csv"
GENERIC_QUERY = """
select
    *
from {};
"""

def sql_to_csv(conn, query, table_name):
    query = query.format(table_name)
    data = pd.read_sql(query, con=conn)
    print(f"{table_name} table shape: {data.shape}")
    
    return data

def main():
    # Setup SQLite connection
    conn = sqlite3.connect(DB_FILE)
    cur = conn.cursor()

    # Verify tables exist
    TABLES = [
        "Sales",
        "Customers",
        "Orders",
        "Items",
    ]
    
    for table in TABLES:
        cur.execute(f"select * from {table};").fetchone()
        
    # Load SQL data into CSV
    sales = sql_to_csv(conn, GENERIC_QUERY, "Sales")
    customers = sql_to_csv(conn, GENERIC_QUERY, "Customers")
    orders = sql_to_csv(conn, GENERIC_QUERY, "Orders")
    items = sql_to_csv(conn, GENERIC_QUERY, "Items")
    
    # Create subset of customers within age range
    customers_subset = customers.loc[customers["age"].between(18, 35)]
        
    # Join sales, orders and items data to customer data to acquire item quantities
    customers_items = customers_subset.merge(sales, on="customer_id") \
                                        .merge(orders, on="sales_id") \
                                        .merge(items, on="item_id")
                                        
    # Filter out any null quantities
    customers_items = customers_items.loc[customers_items["quantity"].notna()]
    
    # Compute for total quantity of each item ordered by a customer
    total_items_per_customer = customers_items.groupby(["customer_id", "age", "item_name"]).sum()[["quantity"]].reset_index()
    total_items_per_customer = total_items_per_customer.rename(columns={
        "customer_id": "Customer",
        "age": "Age",
        "item_name": "Item",
        "quantity": "Quantity",
    })
    total_items_per_customer["Quantity"] = total_items_per_customer["Quantity"].astype(int)
    total_items_per_customer.to_csv(SOLUTION_CSV, ";", index=False)
    
    conn.close()
    
if __name__ == "__main__":
    main()