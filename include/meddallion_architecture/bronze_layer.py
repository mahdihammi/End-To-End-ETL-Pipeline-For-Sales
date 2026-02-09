# from datetime import datetime
# import pandas as pd
# from include.helpers.helper import ensure_bucket, write_parquet
# from duckdb_provider.hooks.duckdb_hook import DuckDBHook


# RAW_BUCKET = "raw"

# def raw_orders(execution_date=None):

#     ensure_bucket(RAW_BUCKET)

#     df = pd.read_csv(
#         "/usr/local/airflow/include/data.csv"
#     )

#     # Use Airflow execution_date if available, otherwise today
#     if execution_date:
#         partition_date = execution_date.strftime("%Y-%m-%d")
#     else:
#         partition_date = datetime.utcnow().strftime("%Y-%m-%d")

#     object_path = f"orders/{partition_date}/orders_raw.parquet"

#     write_parquet(
#         df,
#         RAW_BUCKET,
#         object_path,
#     )


# def bronze_table(LOCAL_DUCKDB_CONN_ID, LOCAL_DUCKDB_TABLE_NAME, RAW_BUCKET):
#     """
#     Create bronze table in DuckDB from raw data in MinIO.
#     """
#     my_duck_hook = DuckDBHook.get_hook(LOCAL_DUCKDB_CONN_ID)
#     conn = my_duck_hook.get_conn()

#     # Define the path to the raw parquet file in MinIO
#     raw_parquet_path = f"s3://{RAW_BUCKET}/orders/*/orders_raw.parquet"

#     # Install and load httpfs extension
#     conn.execute("INSTALL httpfs;")
#     conn.execute("LOAD httpfs;")

#     # Configure S3 settings for MinIO
#     conn.execute("""
#         SET s3_endpoint='minio:9000';
#         SET s3_access_key_id='minioadmin';
#         SET s3_secret_access_key='minioadmin';
#         SET s3_use_ssl=false;
#         SET s3_url_style='path';
#     """)

#     try:
#         # Create the bronze table if it doesn't exist
#         conn.execute(
#             f"""
#             CREATE TABLE IF NOT EXISTS bronze.{LOCAL_DUCKDB_TABLE_NAME} AS
#             SELECT * FROM read_parquet('{raw_parquet_path}');
#             """
#         )

#         # Test print to verify data load
#         result = conn.execute(f"SELECT * FROM {LOCAL_DUCKDB_TABLE_NAME} LIMIT 5;").fetchall()
#         print("Bronze Table Sample Data:", result)
#     except Exception as e:
#         print(f"Error creating bronze table: {e}")
#         # Try to list what's actually in the bucket for debugging
#         try:
#             test_result = conn.execute(f"SELECT * FROM read_parquet('s3://{RAW_BUCKET}/orders/*/orders_raw.parquet');").fetchall()
#             print("Direct read test:", test_result)
#         except Exception as e2:
#             print(f"Direct read also failed: {e2}")
#         raise
#     finally:
#         conn.close()