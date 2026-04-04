"""Unit tests for seed data generation and MinIO upload."""
from unittest.mock import MagicMock, call

import pandas as pd
import pytest

from text_to_sql.database.seed import generate_dataframes, seed


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
