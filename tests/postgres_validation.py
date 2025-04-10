# tests/postgres_validation.py

from src.data.database import PostgresConnection, get_postgres_cursor

# function to count records in each table
def count_table_records(conn, schema, tables):
    results = {}
    cursor = get_postgres_cursor(conn)

    for table in tables:
        table_name = f"{schema}.{table}"
        cursor.execute(f"SELECT COUNT(*) FROM {table_name}")
        count = cursor.fetchone()['count']
        results[table] = count

    return results

# connect to PostgreSQL using context manager
with PostgresConnection() as conn:
    # check staging tables
    staging_tables = [
        "stg_customer", 
        "stg_product", 
        "stg_transaction", 
        "stg_transaction_item"
    ]
    staging_counts = count_table_records(conn, "staging", staging_tables)

    print("Staging table record counts:")
    for table, count in staging_counts.items():
        print(f"   {table}: {count}")

    # check retail dimension tables
    dim_tables = [
        "dim_customer", 
        "dim_product", 
        "dim_location", 
        "dim_date"
    ]
    dim_counts = count_table_records(conn, "retail", dim_tables)

    print("\nDimension table record counts:")
    for table, count in dim_counts.items():
        print(f"   {table}: {count}")

    # check retail fact tables
    fact_tables = [
        "fact_transaction",
        "fact_transaction_item"
    ]
    fact_counts = count_table_records(conn, "retail", fact_tables)

    print("\nFact table record counts:")
    for table, count in fact_counts.items():
        print(f"  {table}: {count}")

    # get sample customer data
    cursor = get_postgres_cursor(conn)
    cursor.execute("SELECT * FROM staging.stg_customer LIMIT 5")
    customers = cursor.fetchall()

    print("\nSample customer data from staging:")
    for customer in customers:
        print(f"   {customer['customer_key']} - {customer['first_name']} {customer['last_name']}")