"""Unit tests for DuckDBExecutor (reads from MinIO Parquet, no SQLite)."""
import io
from datetime import datetime
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from text_to_sql.models.response import ExecutionResult
from text_to_sql.routing.duckdb_executor import DuckDBExecutor

TABLE_NAMES = ["users", "products", "orders", "order_items"]


@pytest.fixture
def mock_minio_client():
    return MagicMock()


@pytest.fixture
def in_memory_conn():
    """DuckDB in-memory connection pre-loaded with test data (no MinIO needed)."""
    conn = duckdb.connect()
    conn.register(
        "users",
        pd.DataFrame({"user_id": list(range(1, 21)), "username": [f"u{i}" for i in range(1, 21)]}),
    )
    conn.register(
        "orders",
        pd.DataFrame(
            {
                "order_id": list(range(1, 61)),
                "user_id": [i % 20 + 1 for i in range(60)],
                "status": ["paid"] * 60,
                "total_amount": [float(i * 10) for i in range(1, 61)],
            }
        ),
    )
    yield conn
    conn.close()


@pytest.fixture
def executor(mock_minio_client, in_memory_conn):
    return DuckDBExecutor(
        minio_client=mock_minio_client,
        bucket="test-bucket",
        table_names=TABLE_NAMES,
        conn=in_memory_conn,
    )


@pytest.fixture
def executor_no_conn(mock_minio_client):
    """Executor without injected conn (uses MinIO S3 path flow)."""
    return DuckDBExecutor(
        minio_client=mock_minio_client,
        bucket="test-bucket",
        table_names=TABLE_NAMES,
    )


class TestExecuteReturnsExecutionResult:
    def test_returns_execution_result(self, executor):
        result = executor.execute("SELECT * FROM users WHERE user_id = 1", ["users"])
        assert isinstance(result, ExecutionResult)

    def test_success_flag_true_on_valid_sql(self, executor):
        result = executor.execute("SELECT * FROM users WHERE user_id = 1", ["users"])
        assert result.success is True

    def test_columns_populated(self, executor):
        result = executor.execute("SELECT user_id, username FROM users LIMIT 1", ["users"])
        assert "user_id" in result.columns
        assert "username" in result.columns

    def test_rows_populated_for_small_result(self, executor):
        result = executor.execute("SELECT * FROM users WHERE user_id <= 3", ["users"])
        assert len(result.rows) == 3
        assert result.row_count == 3

    def test_error_flag_on_invalid_sql(self, executor):
        result = executor.execute("SELECT * FROM nonexistent_table", ["users"])
        assert result.success is False
        assert result.error is not None


class TestCsvThreshold:
    def test_inline_when_rows_below_threshold(self, executor):
        # orders has 60 rows but we filter to 3
        result = executor.execute("SELECT * FROM orders WHERE order_id <= 3", ["orders"])
        assert result.csv_url is None
        assert len(result.rows) == 3

    def test_csv_url_set_when_rows_exceed_threshold(self, executor, mock_minio_client):
        # orders has 60 rows — exceeds threshold of 50
        result = executor.execute("SELECT * FROM orders", ["orders"])
        assert result.csv_url is not None
        assert result.rows == []
        assert result.row_count == 60

    def test_csv_url_contains_results_prefix(self, executor, mock_minio_client):
        result = executor.execute("SELECT * FROM orders", ["orders"])
        assert result.csv_url.startswith("results/")

    def test_csv_url_ends_with_csv(self, executor, mock_minio_client):
        result = executor.execute("SELECT * FROM orders", ["orders"])
        assert result.csv_url.endswith(".csv")

    def test_minio_put_object_called_when_large_result(self, executor, mock_minio_client):
        executor.execute("SELECT * FROM orders", ["orders"])
        mock_minio_client.put_object.assert_called_once()

    def test_minio_not_called_for_small_result(self, executor, mock_minio_client):
        executor.execute("SELECT * FROM orders WHERE order_id <= 3", ["orders"])
        mock_minio_client.put_object.assert_not_called()

    def test_row_count_reflects_total_even_when_csv(self, executor):
        result = executor.execute("SELECT * FROM orders", ["orders"])
        assert result.row_count == 60

    def test_threshold_boundary_at_50_is_inline(self, executor):
        result = executor.execute("SELECT * FROM orders WHERE order_id <= 50", ["orders"])
        assert result.csv_url is None
        assert result.row_count == 50

    def test_threshold_boundary_at_51_triggers_csv(self, executor):
        result = executor.execute("SELECT * FROM orders WHERE order_id <= 51", ["orders"])
        assert result.csv_url is not None
        assert result.row_count == 51


class TestS3PathMounting:
    """Tests for the S3-path mounting flow (no injected conn)."""

    def test_execute_in_duckdb_with_s3_paths_mounts_views(self, executor_no_conn):
        mock_df = pd.DataFrame({"user_id": [1, 2], "username": ["a", "b"]})
        with patch.object(executor_no_conn, "_execute_in_duckdb", return_value=mock_df) as mock_exec:
            executor_no_conn.execute("SELECT * FROM users WHERE user_id = 1", ["users"])
            mock_exec.assert_called_once()
            call_args = mock_exec.call_args
            sql_arg = call_args[0][0]
            paths_arg = call_args[0][1]
            assert sql_arg == "SELECT * FROM users WHERE user_id = 1"
            assert "users" in paths_arg
            assert paths_arg["users"] == "s3://test-bucket/users.parquet"

    def test_s3_paths_built_for_all_table_refs(self, executor_no_conn):
        mock_df = pd.DataFrame()
        with patch.object(executor_no_conn, "_execute_in_duckdb", return_value=mock_df):
            executor_no_conn.execute("SELECT * FROM orders JOIN users ON 1=1", ["orders", "users"])

    def test_execute_in_duckdb_configures_s3(self, executor_no_conn):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({"id": [1]})
        with patch("text_to_sql.routing.duckdb_executor.duckdb") as mock_duckdb:
            mock_duckdb.connect.return_value.__enter__ = MagicMock(return_value=mock_conn)
            mock_duckdb.connect.return_value.__exit__ = MagicMock(return_value=False)
            executor_no_conn._execute_in_duckdb(
                "SELECT * FROM users",
                {"users": "s3://test-bucket/users.parquet"},
            )
        # httpfs + s3 config + view + query = at least 4 calls
        assert mock_conn.execute.call_count >= 4
