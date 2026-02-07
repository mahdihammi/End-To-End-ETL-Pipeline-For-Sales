from datetime import datetime
from json import load
import pandas as pd
from dags.pipeline import SILVER_TABLE_NAME
from duckdb_provider.hooks.duckdb_hook import DuckDBHook
from include.helpers.sql_helper import load_sql
from include.helpers.helper import ensure_bucket, write_parquet


SILVER_BUCKET = "silver"
SILVER_SCHEMA = "silver"

def silver_table(LOCAL_DUCKDB_CONN_ID, SILVER_TABLE_NAME, mode='overwrite'):
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
                create_silver_table(conn, SILVER_TABLE_NAME)
                
        else:
            print(f"🆕 Table '{SILVER_TABLE_NAME}' does not exist - Creating new table")
            create_silver_table(conn, SILVER_TABLE_NAME)
        
        # Verify the result
        verify_silver_table(conn, SILVER_TABLE_NAME)

        # Export to MinIO
        
    except Exception as e:
        print(f"❌ Error creating silver table: {e}")
        raise
    finally:
        conn.close()

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
    



def create_silver_table(conn, silver_table):
    """
    Create silver table from bronze data.
    """
    print(f"Creating table: {silver_table}")

    query = load_sql("silver_transformation.sql")

    print("Executing SQL Query:")
    print(query)

    conn.execute(query)

    print(f"✅ Table silver.{silver_table} created successfully")   