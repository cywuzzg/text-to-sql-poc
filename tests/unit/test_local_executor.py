"""Unit tests for LocalDuckDBExecutor (no MinIO required)."""
import os
from pathlib import Path
from unittest.mock import MagicMock, patch

import duckdb
import pandas as pd
import pytest

from text_to_sql.routing.local_executor import LocalDuckDBExecutor, CSV_THRESHOLD
from text_to_sql.models.response import ExecutionResult


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_data_dir(tmp_path: Path) -> Path:
    return tmp_path


@pytest.fixture
def small_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with 5 products (below threshold)."""
    conn = duckdb.connect()
    df = pd.DataFrame(
        [(i + 1, f"Product {i + 1}", 100.0 * (i + 1), 10) for i in range(5)],
        columns=["product_id", "name", "price", "stock"],
    )
    conn.register("products", df)
    yield conn
    conn.close()


@pytest.fixture
def large_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with CSV_THRESHOLD + 10 rows (above threshold)."""
    conn = duckdb.connect()
    df = pd.DataFrame(
        [(i + 1, f"Product {i + 1}", 100.0, 10) for i in range(CSV_THRESHOLD + 10)],
        columns=["product_id", "name", "price", "stock"],
    )
    conn.register("products", df)
    yield conn
    conn.close()


@pytest.fixture
def exact_threshold_conn() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB with exactly CSV_THRESHOLD rows (should be inline)."""
    conn = duckdb.connect()
    df = pd.DataFrame(
        [(i + 1, f"Product {i + 1}", 100.0, 10) for i in range(CSV_THRESHOLD)],
        columns=["product_id", "name", "price", "stock"],
    )
    conn.register("products", df)
    yield conn
    conn.close()


# ---------------------------------------------------------------------------
# Tests: constructor / basics
# ---------------------------------------------------------------------------

class TestLocalDuckDBExecutorInit:
    def test_init_with_conn_does_not_require_parquet_files(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        assert executor is not None

    def test_data_dir_stored_as_path(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        assert executor._data_dir == tmp_data_dir

    def test_data_dir_accepts_string(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=str(tmp_data_dir), conn=small_conn)
        assert executor._data_dir == tmp_data_dir


# ---------------------------------------------------------------------------
# Tests: small result (inline rows)
# ---------------------------------------------------------------------------

class TestLocalDuckDBExecutorSmallResult:
    def test_returns_execution_result_type(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert isinstance(result, ExecutionResult)

    def test_success_true_for_valid_query(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.success is True

    def test_columns_match_dataframe(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.columns == ["product_id", "name", "price", "stock"]

    def test_rows_are_inline_for_small_result(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert len(result.rows) == 5
        assert result.row_count == 5

    def test_csv_url_is_none_for_small_result(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.csv_url is None

    def test_exact_threshold_returns_inline(self, tmp_data_dir, exact_threshold_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=exact_threshold_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert len(result.rows) == CSV_THRESHOLD
        assert result.csv_url is None


# ---------------------------------------------------------------------------
# Tests: large result (CSV file)
# ---------------------------------------------------------------------------

class TestLocalDuckDBExecutorLargeResult:
    def test_rows_empty_for_large_result(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.rows == []

    def test_row_count_correct_for_large_result(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.row_count == CSV_THRESHOLD + 10

    def test_csv_url_set_for_large_result(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.csv_url is not None

    def test_csv_url_ends_with_dot_csv(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert result.csv_url.endswith(".csv")

    def test_csv_file_exists_on_disk(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert Path(result.csv_url).exists()

    def test_csv_file_has_correct_row_count(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        saved_df = pd.read_csv(result.csv_url)
        assert len(saved_df) == CSV_THRESHOLD + 10

    def test_csv_saved_in_results_subdir(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert "results" in result.csv_url

    def test_csv_url_is_absolute_path(self, tmp_data_dir, large_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=large_conn)
        result = executor.execute("SELECT * FROM products", ["products"])
        assert Path(result.csv_url).is_absolute()


# ---------------------------------------------------------------------------
# Tests: error handling
# ---------------------------------------------------------------------------

class TestLocalDuckDBExecutorErrors:
    def test_invalid_sql_returns_failure(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM nonexistent_table", ["products"])
        assert result.success is False

    def test_invalid_sql_sets_error_message(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("SELECT * FROM nonexistent_table", ["products"])
        assert result.error is not None
        assert len(result.error) > 0

    def test_invalid_sql_returns_empty_columns_and_rows(self, tmp_data_dir, small_conn):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir, conn=small_conn)
        result = executor.execute("NOT VALID SQL", ["products"])
        assert result.columns == []
        assert result.rows == []
        assert result.row_count == 0


# ---------------------------------------------------------------------------
# Tests: reading from local Parquet files (no injected conn)
# ---------------------------------------------------------------------------

class TestLocalPersistentConnection:
    """Tests for connection reuse across multiple queries."""

    @pytest.fixture
    def products_parquet(self, tmp_data_dir) -> Path:
        df = pd.DataFrame(
            [(1, "Widget", 9.99, 5)],
            columns=["product_id", "name", "price", "stock"],
        )
        df.to_parquet(tmp_data_dir / "products.parquet", index=False)
        return tmp_data_dir

    def test_persistent_conn_is_none_initially(self, tmp_data_dir):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir)
        assert executor._persistent_conn is None

    def test_persistent_conn_set_after_first_query(self, products_parquet):
        executor = LocalDuckDBExecutor(data_dir=products_parquet)
        executor.execute("SELECT * FROM products", ["products"])
        assert executor._persistent_conn is not None

    def test_same_conn_object_reused_on_second_query(self, products_parquet):
        executor = LocalDuckDBExecutor(data_dir=products_parquet)
        executor.execute("SELECT * FROM products", ["products"])
        first_conn = executor._persistent_conn
        executor.execute("SELECT * FROM products", ["products"])
        assert executor._persistent_conn is first_conn

    def test_execute_uses_create_or_replace_view(self, tmp_data_dir):
        mock_conn = MagicMock()
        mock_conn.execute.return_value.df.return_value = pd.DataFrame({"id": [1]})
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir)
        with patch.object(executor, "_get_or_create_conn", return_value=mock_conn):
            executor._execute_from_parquet("SELECT * FROM products", ["products"])
        calls = [str(c) for c in mock_conn.execute.call_args_list]
        assert any("CREATE OR REPLACE VIEW" in c for c in calls)


class TestLocalDuckDBExecutorFromParquet:
    def test_reads_parquet_file_without_injected_conn(self, tmp_data_dir):
        # Write a small Parquet file to the data dir
        df = pd.DataFrame(
            [(1, "Widget", 9.99, 5), (2, "Gadget", 19.99, 3)],
            columns=["product_id", "name", "price", "stock"],
        )
        df.to_parquet(tmp_data_dir / "products.parquet", index=False)

        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir)
        result = executor.execute("SELECT * FROM products ORDER BY product_id", ["products"])
        assert result.success is True
        assert result.row_count == 2
        assert result.rows[0][1] == "Widget"

    def test_missing_parquet_file_returns_failure(self, tmp_data_dir):
        executor = LocalDuckDBExecutor(data_dir=tmp_data_dir)
        result = executor.execute("SELECT * FROM users", ["users"])
        assert result.success is False
        assert result.error is not None
