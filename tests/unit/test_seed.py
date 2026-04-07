"""Unit tests for seed data generation and MinIO upload."""
import sqlite3
from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from text_to_sql.database.seed import generate_dataframes, seed, seed_sqlite


class TestGenerateDataframes:
    def test_returns_all_four_tables(self):
        dfs = generate_dataframes()
        assert set(dfs.keys()) == {"users", "products", "orders", "order_items"}

    def test_users_count(self):
        dfs = generate_dataframes()
        assert len(dfs["users"]) == 20

    def test_products_count(self):
        dfs = generate_dataframes()
        assert len(dfs["products"]) == 30

    def test_orders_count(self):
        dfs = generate_dataframes()
        assert len(dfs["orders"]) == 60

    def test_order_items_count(self):
        dfs = generate_dataframes()
        assert len(dfs["order_items"]) >= 60  # at least 1 item per order

    def test_users_columns(self):
        dfs = generate_dataframes()
        assert set(dfs["users"].columns) >= {"user_id", "username", "email", "region"}

    def test_products_columns(self):
        dfs = generate_dataframes()
        assert set(dfs["products"].columns) >= {"product_id", "name", "category", "price", "stock"}

    def test_orders_columns(self):
        dfs = generate_dataframes()
        assert set(dfs["orders"].columns) >= {"order_id", "user_id", "status", "total_amount"}

    def test_order_items_columns(self):
        dfs = generate_dataframes()
        assert set(dfs["order_items"].columns) >= {
            "item_id", "order_id", "product_id", "quantity", "unit_price"
        }

    def test_reproducible_with_same_seed(self):
        dfs1 = generate_dataframes(seed_value=42)
        dfs2 = generate_dataframes(seed_value=42)
        pd.testing.assert_frame_equal(dfs1["orders"], dfs2["orders"])

    def test_different_seed_produces_different_orders(self):
        dfs1 = generate_dataframes(seed_value=42)
        dfs2 = generate_dataframes(seed_value=99)
        # status distribution will likely differ
        assert not dfs1["orders"]["status"].equals(dfs2["orders"]["status"])

    def test_user_ids_are_sequential(self):
        dfs = generate_dataframes()
        assert list(dfs["users"]["user_id"]) == list(range(1, 21))

    def test_product_ids_are_sequential(self):
        dfs = generate_dataframes()
        assert list(dfs["products"]["product_id"]) == list(range(1, 31))

    def test_region_values_are_valid(self):
        dfs = generate_dataframes()
        valid = {"north", "south", "east", "west"}
        assert set(dfs["users"]["region"].unique()).issubset(valid)

    def test_order_status_values_are_valid(self):
        dfs = generate_dataframes()
        valid = {"pending", "paid", "shipped", "delivered", "cancelled"}
        assert set(dfs["orders"]["status"].unique()).issubset(valid)

    def test_created_at_is_datetime_dtype_in_users(self):
        """Regression: created_at must be stored as datetime64, not string/object."""
        dfs = generate_dataframes()
        assert pd.api.types.is_datetime64_any_dtype(dfs["users"]["created_at"]), (
            f"users.created_at dtype is {dfs['users']['created_at'].dtype}, expected datetime64"
        )

    def test_created_at_is_datetime_dtype_in_products(self):
        dfs = generate_dataframes()
        assert pd.api.types.is_datetime64_any_dtype(dfs["products"]["created_at"])

    def test_created_at_is_datetime_dtype_in_orders(self):
        dfs = generate_dataframes()
        assert pd.api.types.is_datetime64_any_dtype(dfs["orders"]["created_at"])

    def test_total_amount_matches_items(self):
        dfs = generate_dataframes()
        for _, order in dfs["orders"].iterrows():
            items = dfs["order_items"][dfs["order_items"]["order_id"] == order["order_id"]]
            expected = (items["quantity"] * items["unit_price"]).sum()
            assert abs(order["total_amount"] - expected) < 0.01


class TestSeedUpload:
    @pytest.fixture
    def mock_minio(self):
        return MagicMock()

    def test_seed_calls_put_object_for_each_table(self, mock_minio):
        seed(minio_client=mock_minio, bucket="test-bucket")
        assert mock_minio.put_object.call_count == 4

    def test_seed_uploads_users_parquet(self, mock_minio):
        seed(minio_client=mock_minio, bucket="test-bucket")
        object_names = [
            call_args[1].get("object_name") or call_args[0][1]
            for call_args in mock_minio.put_object.call_args_list
        ]
        assert "users.parquet" in object_names

    def test_seed_uploads_all_table_parquets(self, mock_minio):
        seed(minio_client=mock_minio, bucket="test-bucket")
        object_names = [
            call_args[1].get("object_name") or call_args[0][1]
            for call_args in mock_minio.put_object.call_args_list
        ]
        for expected in ["users.parquet", "products.parquet", "orders.parquet", "order_items.parquet"]:
            assert expected in object_names

    def test_seed_uses_correct_bucket(self, mock_minio):
        seed(minio_client=mock_minio, bucket="my-bucket")
        for call_args in mock_minio.put_object.call_args_list:
            bucket = call_args[1].get("bucket_name") or call_args[0][0]
            assert bucket == "my-bucket"


class TestSeedSQLite:
    @pytest.fixture
    def in_memory_db(self):
        conn = sqlite3.connect(":memory:")
        conn.execute("PRAGMA foreign_keys = ON")
        conn.executescript("""
            CREATE TABLE users (
                user_id    INTEGER PRIMARY KEY,
                username   VARCHAR NOT NULL UNIQUE,
                email      VARCHAR NOT NULL UNIQUE,
                created_at TIMESTAMP NOT NULL,
                region     VARCHAR
            );
            CREATE TABLE products (
                product_id INTEGER PRIMARY KEY,
                name       VARCHAR NOT NULL,
                category   VARCHAR NOT NULL,
                price      DOUBLE NOT NULL,
                stock      INTEGER NOT NULL DEFAULT 0,
                created_at TIMESTAMP NOT NULL
            );
            CREATE TABLE orders (
                order_id     INTEGER PRIMARY KEY,
                user_id      INTEGER NOT NULL REFERENCES users(user_id),
                status       VARCHAR NOT NULL,
                total_amount DOUBLE NOT NULL,
                created_at   TIMESTAMP NOT NULL
            );
            CREATE TABLE order_items (
                item_id    INTEGER PRIMARY KEY,
                order_id   INTEGER NOT NULL REFERENCES orders(order_id),
                product_id INTEGER NOT NULL REFERENCES products(product_id),
                quantity   INTEGER NOT NULL,
                unit_price DOUBLE NOT NULL
            );
        """)
        yield conn
        conn.close()

    def test_seed_sqlite_inserts_correct_user_count(self, in_memory_db):
        seed_sqlite(in_memory_db)
        count = in_memory_db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        assert count == 20

    def test_seed_sqlite_inserts_correct_product_count(self, in_memory_db):
        seed_sqlite(in_memory_db)
        count = in_memory_db.execute("SELECT COUNT(*) FROM products").fetchone()[0]
        assert count == 30

    def test_seed_sqlite_inserts_correct_order_count(self, in_memory_db):
        seed_sqlite(in_memory_db)
        count = in_memory_db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert count == 60

    def test_seed_sqlite_inserts_order_items(self, in_memory_db):
        seed_sqlite(in_memory_db)
        count = in_memory_db.execute("SELECT COUNT(*) FROM order_items").fetchone()[0]
        assert count >= 60

    def test_seed_sqlite_is_idempotent(self, in_memory_db):
        """Running twice should produce the same row counts (no duplicates)."""
        seed_sqlite(in_memory_db)
        seed_sqlite(in_memory_db)
        user_count = in_memory_db.execute("SELECT COUNT(*) FROM users").fetchone()[0]
        order_count = in_memory_db.execute("SELECT COUNT(*) FROM orders").fetchone()[0]
        assert user_count == 20
        assert order_count == 60

    def test_seed_sqlite_created_at_stored_as_string(self, in_memory_db):
        """SQLite stores TIMESTAMP as ISO-format string."""
        seed_sqlite(in_memory_db)
        row = in_memory_db.execute("SELECT created_at FROM users LIMIT 1").fetchone()
        assert isinstance(row[0], str), f"Expected str, got {type(row[0])}"
        # Should be parseable as ISO datetime
        from datetime import datetime
        datetime.fromisoformat(row[0])  # raises if not valid ISO format
