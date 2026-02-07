# include/medallion/helper.py

from minio import Minio
import pandas as pd
import io

MINIO_ENDPOINT = "minio:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"

# RAW_BUCKET = "raw"
# SILVER_BUCKET = "silver"
# GOLD_BUCKET = "gold"

client = Minio(
    MINIO_ENDPOINT,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False,
)

def ensure_bucket(bucket: str):
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

def write_parquet(df: pd.DataFrame, bucket: str, path: str):
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False)
    buffer.seek(0)

    client.put_object(
        bucket_name=bucket,
        object_name=path,
        data=buffer,
        length=buffer.getbuffer().nbytes,
        content_type="application/octet-stream",
    )

def read_parquet(bucket: str, path: str) -> pd.DataFrame:
    obj = client.get_object(bucket, path)
    return pd.read_parquet(io.BytesIO(obj.read()))
