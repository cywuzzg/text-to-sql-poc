"""Upload seed Parquet files to MinIO to initialise the data store."""
import os
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from minio import Minio
from minio.error import S3Error

from text_to_sql.config import MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
from text_to_sql.database.seed import seed


def init_data(
    bucket: str = MINIO_BUCKET,
    endpoint: str = MINIO_ENDPOINT,
    access_key: str = MINIO_ACCESS_KEY,
    secret_key: str = MINIO_SECRET_KEY,
) -> None:
    client = Minio(endpoint=endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)
        print(f"Created bucket: {bucket}")

    seed(minio_client=client, bucket=bucket)
    print(f"Seed data uploaded to MinIO bucket '{bucket}' (endpoint: {endpoint})")
    print("Tables: users.parquet, products.parquet, orders.parquet, order_items.parquet")


if __name__ == "__main__":
    init_data()
