"""Generate seed DataFrames and upload to MinIO as Parquet files."""
import io
import random
from datetime import datetime, timedelta
from pathlib import Path
from typing import Dict, List, Tuple

import duckdb
import pandas as pd

from text_to_sql.database.schema import DDL_STATEMENTS

USERS: List[Tuple] = [
    ("alice", "alice@example.com", "north"),
    ("bob", "bob@example.com", "south"),
    ("carol", "carol@example.com", "east"),
    ("dave", "dave@example.com", "west"),
    ("eve", "eve@example.com", "north"),
    ("frank", "frank@example.com", "south"),
    ("grace", "grace@example.com", "east"),
    ("henry", "henry@example.com", "west"),
    ("iris", "iris@example.com", "north"),
    ("jack", "jack@example.com", "south"),
    ("karen", "karen@example.com", "east"),
    ("leo", "leo@example.com", "west"),
    ("mary", "mary@example.com", "north"),
    ("nick", "nick@example.com", "south"),
    ("olivia", "olivia@example.com", "east"),
    ("peter", "peter@example.com", "west"),
    ("quinn", "quinn@example.com", "north"),
    ("rose", "rose@example.com", "south"),
    ("sam", "sam@example.com", "east"),
    ("tina", "tina@example.com", "west"),
]

PRODUCTS: List[Tuple] = [
    ("iPhone 15", "electronics", 29900.0, 50),
    ("Samsung Galaxy S24", "electronics", 24900.0, 30),
    ("MacBook Air M3", "electronics", 42900.0, 20),
    ("AirPods Pro", "electronics", 7490.0, 100),
    ("iPad Mini", "electronics", 15900.0, 40),
    ("Sony WH-1000XM5", "electronics", 9900.0, 60),
    ("Nintendo Switch", "electronics", 9980.0, 35),
    ("Kindle Paperwhite", "electronics", 4490.0, 80),
    ("夏季棉T-恤（白）", "clothing", 299.0, 200),
    ("牛仔褲（深藍）", "clothing", 890.0, 150),
    ("運動外套", "clothing", 1290.0, 90),
    ("連帽上衣", "clothing", 690.0, 120),
    ("休閒短褲", "clothing", 490.0, 180),
    ("polo衫", "clothing", 590.0, 100),
    ("風衣", "clothing", 2490.0, 45),
    ("運動鞋", "clothing", 1890.0, 70),
    ("有機米（5kg）", "food", 350.0, 300),
    ("橄欖油（500ml）", "food", 280.0, 200),
    ("黑巧克力禮盒", "food", 450.0, 150),
    ("綠茶茶葉（100g）", "food", 380.0, 200),
    ("咖啡豆（250g）", "food", 520.0, 180),
    ("蜂蜜（500g）", "food", 290.0, 250),
    ("果乾綜合包", "food", 199.0, 300),
    ("燕麥片（1kg）", "food", 159.0, 400),
    ("北歐風桌燈", "home", 1290.0, 60),
    ("記憶棉枕頭", "home", 890.0, 80),
    ("香氛蠟燭組", "home", 590.0, 100),
    ("收納盒六件組", "home", 490.0, 120),
    ("竹製砧板", "home", 350.0, 150),
    ("不鏽鋼保溫瓶", "home", 680.0, 200),
]

_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
_STATUS_WEIGHTS = [0.1, 0.2, 0.3, 0.35, 0.05]


def _random_date(now: datetime, days_ago_max: int = 90) -> datetime:
    delta = timedelta(days=random.randint(0, days_ago_max))
    return now - delta


def generate_dataframes(seed_value: int = 42) -> Dict[str, pd.DataFrame]:
    """Generate seed DataFrames for all four tables.

    Args:
        seed_value: Random seed for reproducibility.

    Returns:
        Dict mapping table name → DataFrame.
    """
    random.seed(seed_value)
    # Fix the reference time so timestamps are fully reproducible for the same seed_value.
    now = datetime.now().replace(microsecond=0)

    users_df = pd.DataFrame(
        [
            (i + 1, username, email, _random_date(now), region)
            for i, (username, email, region) in enumerate(USERS)
        ],
        columns=["user_id", "username", "email", "created_at", "region"],
    )

    products_df = pd.DataFrame(
        [
            (i + 1, name, category, price, stock, _random_date(now))
            for i, (name, category, price, stock) in enumerate(PRODUCTS)
        ],
        columns=["product_id", "name", "category", "price", "stock", "created_at"],
    )

    product_prices = dict(zip(products_df["product_id"], products_df["price"]))
    user_ids = list(users_df["user_id"])
    product_ids = list(products_df["product_id"])

    orders_rows = []
    order_items_rows = []
    order_id = 1
    item_id = 1

    for _ in range(60):
        uid = random.choice(user_ids)
        status = random.choices(_STATUSES, weights=_STATUS_WEIGHTS)[0]
        created = _random_date(now)

        items_count = random.randint(1, 3)
        chosen_products = random.sample(product_ids, items_count)
        quantities = [random.randint(1, 4) for _ in chosen_products]
        prices = [product_prices[pid] for pid in chosen_products]
        total = sum(q * p for q, p in zip(quantities, prices))

        orders_rows.append((order_id, uid, status, total, created))

        for pid, qty, price in zip(chosen_products, quantities, prices):
            order_items_rows.append((item_id, order_id, pid, qty, price))
            item_id += 1

        order_id += 1

    orders_df = pd.DataFrame(
        orders_rows,
        columns=["order_id", "user_id", "status", "total_amount", "created_at"],
    )
    order_items_df = pd.DataFrame(
        order_items_rows,
        columns=["item_id", "order_id", "product_id", "quantity", "unit_price"],
    )

    return {
        "users": users_df,
        "products": products_df,
        "orders": orders_df,
        "order_items": order_items_df,
    }


def seed_sqlite(conn, seed_value: int = 42) -> None:
    """Generate seed data and insert into a SQLite database.

    Clears existing rows before inserting to ensure idempotency.

    Args:
        conn: sqlite3.Connection to the target database.
        seed_value: Random seed for reproducibility.
    """
    dataframes = generate_dataframes(seed_value=seed_value)

    # Clear in FK-safe order
    conn.execute("DELETE FROM order_items")
    conn.execute("DELETE FROM orders")
    conn.execute("DELETE FROM products")
    conn.execute("DELETE FROM users")

    for table_name, df in dataframes.items():
        df.to_sql(table_name, conn, if_exists="append", index=False)

    conn.commit()


def seed_duckdb(db_path, seed_value: int = 42) -> None:
    """Generate seed data and insert into a DuckDB database file.

    Creates the database file and tables if they do not exist.
    Clears existing rows before inserting to ensure idempotency.

    Args:
        db_path: Path (str or Path) to the target .duckdb file.
        seed_value: Random seed for reproducibility.
    """
    db_path = Path(db_path)
    db_path.parent.mkdir(parents=True, exist_ok=True)

    conn = duckdb.connect(str(db_path))
    try:
        for stmt in DDL_STATEMENTS:
            conn.execute(stmt)

        # Clear in FK-safe order
        for table in ["order_items", "orders", "products", "users"]:
            conn.execute(f"DELETE FROM {table}")

        dataframes = generate_dataframes(seed_value=seed_value)
        for table_name, df in dataframes.items():
            conn.register(f"_df_{table_name}", df)
            conn.execute(f"INSERT INTO {table_name} SELECT * FROM _df_{table_name}")
    finally:
        conn.close()


def seed(minio_client, bucket: str, seed_value: int = 42) -> None:
    """Generate seed DataFrames and upload to MinIO as Parquet files.

    Args:
        minio_client: MinIO client instance (minio.Minio).
        bucket: Destination bucket name.
        seed_value: Random seed for reproducibility.
    """
    dataframes = generate_dataframes(seed_value=seed_value)

    for table_name, df in dataframes.items():
        buf = io.BytesIO()
        df.to_parquet(buf, index=False, engine="pyarrow")
        buf.seek(0)
        data_len = buf.getbuffer().nbytes

        minio_client.put_object(
            bucket_name=bucket,
            object_name=f"{table_name}.parquet",
            data=buf,
            length=data_len,
            content_type="application/octet-stream",
        )
