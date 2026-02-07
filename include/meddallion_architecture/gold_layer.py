from datetime import datetime
from duckdb_provider.hooks.duckdb_hook import DuckDBHook


# Dictionary mapping table names to their SQL queries
GOLD_TABLE_QUERIES = {
    "gold_sales_by_time": """
        CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA_NAME}.{table_name} AS
        SELECT 
            order_year,
            order_month,
            order_quarter,
            COUNT(DISTINCT order_id) as total_orders,
            COUNT(DISTINCT customer_id) as unique_customers,
            SUM(sales) as total_sales,
            SUM(profit) as total_profit,
            AVG(profit_margin) as avg_profit_margin,
            SUM(quantity) as total_quantity,
            AVG(sales) as avg_order_value,
            SUM(discount_amount) as total_discounts,
            SUM(CASE WHEN profit_status = 'Profitable' THEN 1 ELSE 0 END) as profitable_orders,
            SUM(CASE WHEN profit_status = 'Loss' THEN 1 ELSE 0 END) as loss_orders,
            CURRENT_TIMESTAMP as created_at
        FROM {SILVER_SCHEMA_NAME}.{silver_table}
        GROUP BY order_year, order_month, order_quarter
        ORDER BY order_year DESC, order_month DESC
    """,
    
    "gold_product_performance": """
        CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA_NAME}.{table_name} AS
        SELECT 
            category,
            sub_category,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(quantity) as total_quantity_sold,
            SUM(sales) as total_sales,
            SUM(profit) as total_profit,
            AVG(profit_margin) as avg_profit_margin,
            AVG(unit_price) as avg_unit_price,
            SUM(discount_amount) as total_discount_given,
            AVG(discount) as avg_discount_rate,
            SUM(sales) / NULLIF(COUNT(DISTINCT order_id), 0) as sales_per_order,
            RANK() OVER (ORDER BY SUM(sales) DESC) as sales_rank,
            RANK() OVER (ORDER BY SUM(profit) DESC) as profit_rank,
            CURRENT_TIMESTAMP as created_at
        FROM {SILVER_SCHEMA_NAME}.{silver_table}
        GROUP BY category, sub_category
        ORDER BY total_sales DESC
    """,
    
    "gold_customer_segments": """
        CREATE TABLE IF NOT EXISTS {GOLD_SCHEMA_NAME}.{table_name} AS
        SELECT 
            segment,
            region,
            COUNT(DISTINCT customer_id) as customer_count,
            COUNT(DISTINCT order_id) as total_orders,
            SUM(sales) as total_sales,
            SUM(profit) as total_profit,
            AVG(profit_margin) as avg_profit_margin,
            SUM(sales) / NULLIF(COUNT(DISTINCT customer_id), 0) as sales_per_customer,
            COUNT(DISTINCT order_id) / NULLIF(COUNT(DISTINCT customer_id), 0) as orders_per_customer,
            AVG(sales) as avg_order_value,
            CURRENT_TIMESTAMP as created_at
        FROM {SILVER_SCHEMA_NAME}.{silver_table}
        GROUP BY segment, region
        ORDER BY total_sales DESC
    """,
    
    # Add other gold tables here...
}


def create_gold_table(LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, GOLD_SCHEMA_NAME, SILVER_SCHEMA_NAME, table_name):
    """
    Create a single gold table.
    This allows parallel execution of multiple gold tables.
    
    Args:
        LOCAL_DUCKDB_CONN_ID: DuckDB connection ID
        SILVER_TABLE_NAME: Source silver table
        table_name: Name of the gold table to create
    """
    my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
    conn = my_duck_hook.get_conn()

    try:
        print(f"Creating {table_name}... in schema {GOLD_SCHEMA_NAME} from table {SILVER_TABLE_NAME} in schema {SILVER_SCHEMA_NAME}")
        
        # Get query for this table
        query_template = GOLD_TABLE_QUERIES.get(table_name)
        
        if not query_template:
            raise ValueError(f"No query defined for table: {table_name}")
        
        # Format query with table names
        query = query_template.format(
            GOLD_SCHEMA_NAME=GOLD_SCHEMA_NAME,
            SILVER_SCHEMA_NAME=SILVER_SCHEMA_NAME,
            table_name=table_name,
            silver_table=SILVER_TABLE_NAME
        )
        print(f"query: {query} for table: {table_name}")
        
        # Execute query
        conn.execute(query)
        
        # Get record count
        count = conn.execute(f"SELECT COUNT(*) FROM {GOLD_SCHEMA_NAME}.{table_name}").fetchone()[0]
        print(f"✅ {table_name}: {count:,} records created")
        
        return count
        
    except Exception as e:
        print(f"❌ Error creating {table_name}: {e}")
        raise
    finally:
        conn.close()


def create_all_gold_tables(LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME):
    """
    Create all gold tables sequentially (original function for backwards compatibility).
    """
    for table_name in GOLD_TABLE_QUERIES.keys():
        create_gold_table(LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, table_name)