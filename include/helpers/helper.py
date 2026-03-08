from minio import Minio
import io
import pandas as pd
import os

# ----------------------------
# CONFIG
# ----------------------------
MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin")
USE_SSL = False  # set True if MinIO is using SSL

# ----------------------------
# MinIO Client
# ----------------------------
def get_minio_client():
    client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=USE_SSL
    )
    return client

# ----------------------------
# Helper functions
# ----------------------------
def upload_parquet(df: pd.DataFrame, bucket_name: str, object_name: str):
    """
    Upload a pandas DataFrame as parquet to MinIO
    """
    client = get_minio_client()

    # Create bucket if not exists
    if not client.bucket_exists(bucket_name):
        client.make_bucket(bucket_name)

    # Save df to BytesIO
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    # Upload
    client.put_object(
        bucket_name=bucket_name,
        object_name=object_name,
        data=buffer,
        length=buffer.getbuffer().nbytes
    )
    print(f"Uploaded {object_name} to bucket {bucket_name}")
