from datetime import datetime
import pandas as pd
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.helper import ensure_bucket, write_parquet
SILVER_BUCKET = "silver"
SILVER_SCHEMA = "silver"

def silver_table(LOCAL_DUCKDB_CONN_ID, BRONZE_TABLE_NAME, SILVER_TABLE_NAME, mode='overwrite'):
    """
    Create or append to silver table with cleaned and enriched data from bronze layer.
    
    Args:
        LOCAL_DUCKDB_CONN_ID: DuckDB connection ID
        BRONZE_TABLE_NAME: Source bronze table name
        SILVER_TABLE_NAME: Target silver table name
        mode: 'overwrite' (default) or 'append' for incremental loads
    """
    my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
    conn = my_duck_hook.get_conn()

    try:
        # Check if table exists
        table_exists = check_table_exists(conn,SILVER_SCHEMA,SILVER_TABLE_NAME)
        
        if table_exists:
            print(f"✅ Table '{SILVER_TABLE_NAME}' already exists")
            
            # Get existing record count
            existing_count = conn.execute(f"SELECT COUNT(*) FROM silver.{SILVER_TABLE_NAME}").fetchone()[0]
            print(f"   Existing records: {existing_count:,}")
            
            if mode == 'overwrite':
                print(f"⚠️  Mode: OVERWRITE - Dropping and recreating table")
                conn.execute(f"DROP TABLE silver.{SILVER_TABLE_NAME}")
                create_silver_table(conn, BRONZE_TABLE_NAME, SILVER_TABLE_NAME)
                
            elif mode == 'append':
                print(f"📝 Mode: APPEND - Adding new records")
                append_to_silver_table(conn, BRONZE_TABLE_NAME, SILVER_TABLE_NAME)
                
        else:
            print(f"🆕 Table '{SILVER_TABLE_NAME}' does not exist - Creating new table")
            create_silver_table(conn, BRONZE_TABLE_NAME, SILVER_TABLE_NAME)
        
        # Verify the result
        verify_silver_table(conn, SILVER_TABLE_NAME)

        # Export to MinIO
        put_silver_orders_minio(silver_table_name=SILVER_TABLE_NAME, LOCAL_DUCKDB_CONN_ID=LOCAL_DUCKDB_CONN_ID)
        
    except Exception as e:
        print(f"❌ Error creating silver table: {e}")
        raise
    finally:
        conn.close()


def check_table_exists(conn, schema_name, table_name):
    """
    Check if a table exists in DuckDB.
    
    Args:
        conn: DuckDB connection
        table_name: Name of the table to check
    
    Returns:
        bool: True if table exists, False otherwise
    """
    try:
        result = conn.execute(f"""
            SELECT COUNT(*) 
            FROM information_schema.tables 
            WHERE table_schema = '{schema_name}' AND table_name = '{table_name}'
        """).fetchone()
        
        return result[0] > 0
    except Exception as e:
        print(f"Error checking table existence: {e}")
        return False


def create_silver_table(conn, bronze_table, silver_table):
    """
    Create silver table from bronze data.
    """
    print(f"Creating table: {silver_table}")
    
    conn.execute(f"""
        CREATE TABLE silver.{silver_table} AS
        SELECT 
        -- Original columns with cleaned names
        "Row ID" as row_id,
        "Order ID" as order_id,
        
        -- Date formatting
        STRPTIME("Order Date", '%d-%m-%Y')::DATE as order_date,
        STRPTIME("Ship Date", '%d-%m-%Y')::DATE as ship_date,
        DATE_DIFF(
            'day',
            STRPTIME("Order Date", '%d-%m-%Y')::DATE,
            STRPTIME("Ship Date", '%d-%m-%Y')::DATE
        ) as shipping_days,
        YEAR(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_year,
        MONTH(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_month,
        QUARTER(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_quarter,
        DAYNAME(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_day_of_week,
        
        -- Shipping and customer info
        "Ship Mode" as ship_mode,
        "Customer ID" as customer_id,
        "Customer Name" as customer_name,
        "Segment" as segment,
        
        -- Location data
        "Country" as country,
        "City" as city,
        "State" as state,
        "Postal Code" as postal_code,
        "Region" as region,
        
        -- Product information
        "Product ID" as product_id,
        "Category" as category,
        "Sub-Category" as sub_category,
        "Product Name" as product_name,
        
        -- Financial metrics
        CAST("Sales" AS DECIMAL(10,2)) as sales,
        CAST("Quantity" AS INTEGER) as quantity,
        CAST("Discount" AS DECIMAL(5,2)) as discount,
        CAST("Profit" AS DECIMAL(10,2)) as profit,
        
        -- Calculated business metrics
        CAST("Sales" AS DECIMAL(10,2)) / NULLIF(CAST("Quantity" AS INTEGER), 0) as unit_price,
        CAST("Profit" AS DECIMAL(10,2)) / NULLIF(CAST("Sales" AS DECIMAL(10,2)), 0) as profit_margin,
        CAST("Sales" AS DECIMAL(10,2)) * CAST("Discount" AS DECIMAL(5,2)) as discount_amount,
        
        -- Categorizations
        CASE 
            WHEN CAST("Profit" AS DECIMAL(10,2)) > 0 THEN 'Profitable'
            WHEN CAST("Profit" AS DECIMAL(10,2)) = 0 THEN 'Break-even'
            ELSE 'Loss'
        END as profit_status,
        
        CASE 
            WHEN CAST("Sales" AS DECIMAL(10,2)) >= 200 THEN 'High Value'
            WHEN CAST("Sales" AS DECIMAL(10,2)) >= 100 THEN 'Medium Value'
            ELSE 'Low Value'
        END as order_value_tier,
        
        CASE 
            WHEN "Ship Mode" = 'Same Day' THEN 0
            WHEN "Ship Mode" = 'First Class' THEN 1
            WHEN "Ship Mode" = 'Second Class' THEN 2
            WHEN "Ship Mode" = 'Standard Class' THEN 3
            ELSE 4
        END as ship_mode_priority,
        
        -- Data quality flags
        CASE 
            WHEN "Sales" IS NULL OR "Quantity" IS NULL OR "Profit" IS NULL THEN TRUE
            ELSE FALSE
        END as has_missing_financial_data,
        
        CASE 
            WHEN DATE_DIFF(
                'day', 
                STRPTIME("Order Date", '%d-%m-%Y')::DATE, 
                STRPTIME("Ship Date", '%d-%m-%Y')::DATE
            ) < 0 THEN TRUE
            ELSE FALSE
        END as is_invalid_ship_date,
        
        -- Metadata
        CURRENT_TIMESTAMP as processed_at
        
        FROM
        bronze.{bronze_table}
        WHERE
        "Order ID" IS NOT NULL
    """)
    
    print(f"✅ Table silver.{silver_table} created successfully")


def append_to_silver_table(conn, bronze_table, silver_table):
    """
    Append new records to existing silver table (incremental load).
    Only inserts records that don't already exist based on order_id and row_id.
    """
    print(f"Appending new records to: {silver_table}")
    
    # Get the max processed_at timestamp from existing data
    max_timestamp = conn.execute(f"""
        SELECT MAX(processed_at) FROM {silver_table}
    """).fetchone()[0]
    
    print(f"   Last processed at: {max_timestamp}")
    
    # Insert only new records that don't exist in silver table
    result = conn.execute(f"""
        INSERT INTO silver.{silver_table}
        SELECT
            -- Original columns with cleaned names
            "Row ID" as row_id,
            "Order ID" as order_id,
            
            -- Date formatting
            STRPTIME("Order Date", '%d-%m-%Y')::DATE as order_date,
            STRPTIME("Ship Date", '%d-%m-%Y')::DATE as ship_date,
            DATE_DIFF(
                'day',
                STRPTIME("Order Date", '%d-%m-%Y')::DATE,
                STRPTIME("Ship Date", '%d-%m-%Y')::DATE
            ) as shipping_days,
            YEAR(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_year,
            MONTH(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_month,
            QUARTER(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_quarter,
            DAYNAME(STRPTIME("Order Date", '%d-%m-%Y')::DATE) as order_day_of_week,
            
            -- Shipping and customer info
            "Ship Mode" as ship_mode,
            "Customer ID" as customer_id,
            "Customer Name" as customer_name,
            "Segment" as segment,
            
            -- Location data
            "Country" as country,
            "City" as city,
            "State" as state,
            "Postal Code" as postal_code,
            "Region" as region,
            
            -- Product information
            "Product ID" as product_id,
            "Category" as category,
            "Sub-Category" as sub_category,
            "Product Name" as product_name,
            
            -- Financial metrics
            CAST("Sales" AS DECIMAL(10,2)) as sales,
            CAST("Quantity" AS INTEGER) as quantity,
            CAST("Discount" AS DECIMAL(5,2)) as discount,
            CAST("Profit" AS DECIMAL(10,2)) as profit,
            
            -- Calculated business metrics
            CAST("Sales" AS DECIMAL(10,2)) / NULLIF(CAST("Quantity" AS INTEGER), 0) as unit_price,
            CAST("Profit" AS DECIMAL(10,2)) / NULLIF(CAST("Sales" AS DECIMAL(10,2)), 0) as profit_margin,
            CAST("Sales" AS DECIMAL(10,2)) * CAST("Discount" AS DECIMAL(5,2)) as discount_amount,
            
            -- Categorizations
            CASE 
                WHEN CAST("Profit" AS DECIMAL(10,2)) > 0 THEN 'Profitable'
                WHEN CAST("Profit" AS DECIMAL(10,2)) = 0 THEN 'Break-even'
                ELSE 'Loss'
            END as profit_status,
            
            CASE 
                WHEN CAST("Sales" AS DECIMAL(10,2)) >= 200 THEN 'High Value'
                WHEN CAST("Sales" AS DECIMAL(10,2)) >= 100 THEN 'Medium Value'
                ELSE 'Low Value'
            END as order_value_tier,
            
            CASE 
                WHEN "Ship Mode" = 'Same Day' THEN 0
                WHEN "Ship Mode" = 'First Class' THEN 1
                WHEN "Ship Mode" = 'Second Class' THEN 2
                WHEN "Ship Mode" = 'Standard Class' THEN 3
                ELSE 4
            END as ship_mode_priority,
            
            -- Data quality flags
            CASE 
                WHEN "Sales" IS NULL OR "Quantity" IS NULL OR "Profit" IS NULL THEN TRUE
                ELSE FALSE
            END as has_missing_financial_data,
            
            CASE 
                WHEN DATE_DIFF(
                    'day', 
                    STRPTIME("Order Date", '%d-%m-%Y')::DATE, 
                    STRPTIME("Ship Date", '%d-%m-%Y')::DATE
                ) < 0 THEN TRUE
                ELSE FALSE
            END as is_invalid_ship_date,
            
            -- Metadata
            CURRENT_TIMESTAMP as processed_at
            
            FROM
            {bronze_table} b
            WHERE
            "Order ID" IS NOT NULL
        AND NOT EXISTS (
            SELECT 1 FROM silver.{silver_table} s
            WHERE s.row_id = b."Row ID"
            AND s.order_id = b."Order ID"
        )
    """)
    
    # Get count of inserted records
    inserted_count = conn.execute(f"""
        SELECT COUNT(*) FROM silver.{silver_table}
        WHERE processed_at > '{max_timestamp}'
    """).fetchone()[0]
    
    print(f"✅ Appended {inserted_count:,} new records")


def verify_silver_table(conn, silver_table):
    """
    Verify and print statistics about the silver table.
    """
    print(f"\n=== Verifying {silver_table} ===")
    # Statistics
    stats = conn.execute(f"""
        SELECT 
            COUNT(*) as total_records,
        FROM silver.{silver_table}
    """).fetchone()
    
    print(f"\n📊 Silver Table Statistics:")
    print(f"   Total records: {stats[0]:,}")



def put_silver_orders_minio(execution_date=None, silver_table_name="orders_silver", LOCAL_DUCKDB_CONN_ID=None):
    """
    Export silver table data to MinIO as Parquet files partitioned by date.
    """

    ensure_bucket(SILVER_BUCKET)
    
    # Use Airflow execution_date if available, otherwise today
    if execution_date:
        partition_date = execution_date.strftime("%Y-%m-%d")
    else:
        partition_date = datetime.utcnow().strftime("%Y-%m-%d")
        print(f"Using current date for partitioning: {partition_date}")

    my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
    conn = my_duck_hook.get_conn()

    conn.execute(f"""
                 
        INSTALL httpfs;
        LOAD httpfs;
        SET s3_endpoint='minio:9000';
        SET s3_access_key_id='minioadmin';
        SET s3_secret_access_key='minioadmin';
        SET s3_use_ssl=false;
        SET s3_url_style='path';
                 

        COPY silver.{silver_table_name}
        TO 's3://silver/orders/2026-01-02/{silver_table_name}.parquet'
        (FORMAT PARQUET);
    """)

    
