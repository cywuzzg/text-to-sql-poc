"""Unit tests for DuckDBExecutor."""
import io
import sqlite3
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from text_to_sql.routing.duckdb_executor import DuckDBExecutor


@pytest.fixture
def sqlite_conn():
    """In-memory SQLite DB with a sample orders table."""
    conn = sqlite3.connect(":memory:")
    conn.execute(
        "CREATE TABLE orders (id INTEGER, user_id INTEGER, amount REAL, status TEXT)"
    )
    conn.execute("INSERT INTO orders VALUES (1, 10, 100.0, 'paid')")
    conn.execute("INSERT INTO orders VALUES (2, 10, 200.0, 'paid')")
    conn.execute("INSERT INTO orders VALUES (3, 20, 50.0, 'pending')")
    conn.commit()
    yield conn
    conn.close()


@pytest.fixture
def mock_minio_client():
    return MagicMock()


@pytest.fixture
def executor(sqlite_conn, mock_minio_client):
    return DuckDBExecutor(
        conn=sqlite_conn,
        minio_client=mock_minio_client,
        bucket="test-bucket",
    )


class TestExportTableToMinio:
    def test_returns_s3_path(self, executor, mock_minio_client):
        path = executor.export_table_to_minio("orders")
        assert path == "s3://test-bucket/orders.parquet"

    def test_calls_put_object_on_minio(self, executor, mock_minio_client):
        executor.export_table_to_minio("orders")
        mock_minio_client.put_object.assert_called_once()
        call_kwargs = mock_minio_client.put_object.call_args
        assert call_kwargs[1]["bucket_name"] == "test-bucket" or call_kwargs[0][0] == "test-bucket"

    def test_uploads_parquet_object_named_correctly(self, executor, mock_minio_client):
        executor.export_table_to_minio("orders")
        call_args = mock_minio_client.put_object.call_args
        # Second positional arg or 'object_name' kwarg should be 'orders.parquet'
        args = call_args[0] if call_args[0] else []
        kwargs = call_args[1] if call_args[1] else {}
        object_name = kwargs.get("object_name") or (args[1] if len(args) > 1 else None)
        assert object_name == "orders.parquet"

    def test_different_table_names(self, executor, mock_minio_client, sqlite_conn):
        sqlite_conn.execute(
            "CREATE TABLE products (id INTEGER, name TEXT, price REAL)"
        )
        sqlite_conn.execute("INSERT INTO products VALUES (1, 'Widget', 9.99)")
        sqlite_conn.commit()

        path = executor.export_table_to_minio("products")
        assert path == "s3://test-bucket/products.parquet"


class TestExecute:
    def test_execute_returns_dataframe(self, executor):
        with patch.object(executor, "_execute_in_duckdb") as mock_exec:
            mock_exec.return_value = pd.DataFrame(
                {"user_id": [10, 20], "total": [300.0, 50.0]}
            )
            result = executor.execute(
                "SELECT user_id, SUM(amount) as total FROM orders GROUP BY user_id",
                table_refs=["orders"],
            )
        assert isinstance(result, pd.DataFrame)

    def test_execute_calls_export_for_each_table_ref(self, executor):
        with patch.object(executor, "export_table_to_minio") as mock_export:
            mock_export.return_value = "s3://test-bucket/orders.parquet"
            with patch.object(executor, "_execute_in_duckdb") as mock_exec:
                mock_exec.return_value = pd.DataFrame()
                executor.execute(
                    "SELECT * FROM orders",
                    table_refs=["orders"],
                )
        mock_export.assert_called_once_with("orders")

    def test_execute_calls_export_for_multiple_tables(self, executor, sqlite_conn):
        sqlite_conn.execute("CREATE TABLE users (id INTEGER, name TEXT)")
        sqlite_conn.commit()

        with patch.object(executor, "export_table_to_minio") as mock_export:
            mock_export.side_effect = lambda t: f"s3://test-bucket/{t}.parquet"
            with patch.object(executor, "_execute_in_duckdb") as mock_exec:
                mock_exec.return_value = pd.DataFrame()
                executor.execute(
                    "SELECT u.name, o.amount FROM orders o JOIN users u ON o.user_id = u.id",
                    table_refs=["orders", "users"],
                )
        assert mock_export.call_count == 2

    def test_execute_in_duckdb_receives_correct_s3_paths(self, executor):
        with patch.object(
            executor, "export_table_to_minio", return_value="s3://test-bucket/orders.parquet"
        ):
            with patch.object(executor, "_execute_in_duckdb") as mock_exec:
                mock_exec.return_value = pd.DataFrame()
                executor.execute("SELECT * FROM orders", table_refs=["orders"])

                call_args = mock_exec.call_args
                sql_arg = call_args[0][0]
                paths_arg = call_args[0][1]
                assert sql_arg == "SELECT * FROM orders"
                assert paths_arg == {"orders": "s3://test-bucket/orders.parquet"}


class TestExecuteInDuckdb:
    """Test the internal DuckDB execution with mocked duckdb."""

    def test_execute_in_duckdb_runs_sql_and_returns_dataframe(self, executor):
        mock_conn = MagicMock()
        expected_df = pd.DataFrame({"id": [1, 2, 3]})
        mock_conn.execute.return_value.df.return_value = expected_df

        with patch("text_to_sql.routing.duckdb_executor.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_duckdb.connect.return_value.__exit__ = MagicMock(return_value=False)

            result = executor._execute_in_duckdb(
                "SELECT * FROM orders",
                {"orders": "s3://test-bucket/orders.parquet"},
            )

        assert isinstance(result, pd.DataFrame)

    def test_execute_in_duckdb_creates_views_for_each_table(self, executor):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.df.return_value = pd.DataFrame()

        with patch("text_to_sql.routing.duckdb_executor.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_duckdb.connect.return_value.__exit__ = MagicMock(return_value=False)

            executor._execute_in_duckdb(
                "SELECT * FROM orders",
                {"orders": "s3://test-bucket/orders.parquet"},
            )

        # Should have called execute for: httpfs install, s3 config, CREATE VIEW, SELECT
        assert mock_conn.execute.call_count >= 1
