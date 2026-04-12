"""Unit tests for DuckDBFileExecutor — reads directly from a .duckdb file."""
from pathlib import Path

import duckdb
import pytest

from text_to_sql.database.schema import DDL_STATEMENTS
from text_to_sql.routing.duckdb_file_executor import DuckDBFileExecutor
from text_to_sql.models.response import ExecutionResult


@pytest.fixture
def duckdb_file(tmp_path) -> Path:
    """Create a small .duckdb file with the four tables and a few rows."""
    db_path = tmp_path / "test.duckdb"
    conn = duckdb.connect(str(db_path))
    for stmt in DDL_STATEMENTS:
        conn.execute(stmt)
    conn.execute("INSERT INTO users VALUES (1, 'alice', 'alice@example.com', NOW(), 'north')")
    conn.execute("INSERT INTO users VALUES (2, 'bob', 'bob@example.com', NOW(), 'south')")
    conn.execute("INSERT INTO products VALUES (1, 'iPhone', 'electronics', 29900.0, 50, NOW())")
    conn.close()
    return db_path


@pytest.fixture
def executor(duckdb_file) -> DuckDBFileExecutor:
    return DuckDBFileExecutor(db_path=duckdb_file)


class TestDuckDBFileExecutorInit:
    def test_init_stores_db_path_as_path(self, duckdb_file):
        ex = DuckDBFileExecutor(db_path=duckdb_file)
        assert ex._db_path == Path(duckdb_file)

    def test_init_accepts_string_path(self, duckdb_file):
        ex = DuckDBFileExecutor(db_path=str(duckdb_file))
        assert ex._db_path == Path(duckdb_file)

    def test_no_persistent_conn_at_init(self, duckdb_file):
        ex = DuckDBFileExecutor(db_path=duckdb_file)
        assert ex._persistent_conn is None


class TestDuckDBFileExecutorExecute:
    def test_execute_returns_execution_result(self, executor):
        result = executor.execute("SELECT * FROM users", ["users"])
        assert isinstance(result, ExecutionResult)

    def test_execute_reads_from_duckdb_file(self, executor):
        result = executor.execute("SELECT COUNT(*) AS n FROM users", ["users"])
        assert result.success is True
        assert result.rows[0][0] == 2

    def test_execute_returns_correct_columns(self, executor):
        result = executor.execute("SELECT user_id, username FROM users ORDER BY user_id", ["users"])
        assert result.success is True
        assert result.columns == ["user_id", "username"]
        assert result.rows[0] == [1, "alice"]

    def test_query_against_empty_db_returns_failure(self, tmp_path):
        """DuckDB creates the file on connect; querying a non-existent table should fail."""
        ex = DuckDBFileExecutor(db_path=tmp_path / "empty.duckdb")
        result = ex.execute("SELECT * FROM users", [])
        assert result.success is False
        assert result.error is not None

    def test_invalid_sql_returns_failure(self, executor):
        result = executor.execute("SELECT * FROM nonexistent_table_xyz", [])
        assert result.success is False
        assert result.error is not None

    def test_table_refs_parameter_accepted_but_unused(self, executor):
        """table_refs is kept for API compatibility but not needed."""
        result = executor.execute("SELECT * FROM users", ["users", "products"])
        assert isinstance(result, ExecutionResult)

    def test_large_result_saved_as_csv(self, tmp_path):
        """Results with >50 rows should be saved as CSV and return csv_url."""
        db_path = tmp_path / "large.duckdb"
        conn = duckdb.connect(str(db_path))
        for stmt in DDL_STATEMENTS:
            conn.execute(stmt)
        # Insert 60 users
        for i in range(60):
            conn.execute(
                f"INSERT INTO users VALUES ({i+1}, 'user{i}', 'user{i}@example.com', NOW(), 'north')"
            )
        conn.close()

        ex = DuckDBFileExecutor(db_path=db_path, results_dir=tmp_path / "results")
        result = ex.execute("SELECT * FROM users", ["users"])
        assert result.success is True
        assert result.row_count == 60
        assert result.csv_url is not None
        assert Path(result.csv_url).exists()

    def test_small_result_returned_inline(self, executor):
        result = executor.execute("SELECT * FROM users", ["users"])
        assert result.success is True
        assert len(result.rows) == 2
        assert result.csv_url is None


class TestDuckDBFileExecutorPersistentConnection:
    def test_persistent_conn_created_after_first_query(self, executor):
        assert executor._persistent_conn is None
        executor.execute("SELECT 1", [])
        assert executor._persistent_conn is not None

    def test_persistent_conn_reused_across_queries(self, executor):
        executor.execute("SELECT 1", [])
        conn_first = executor._persistent_conn
        executor.execute("SELECT 2", [])
        assert executor._persistent_conn is conn_first

    def test_injected_conn_used_instead_of_file(self, duckdb_file):
        """When conn is injected, _db_path file is not opened."""
        mock_conn = duckdb.connect()
        mock_conn.execute("CREATE TABLE t (x INTEGER)")
        mock_conn.execute("INSERT INTO t VALUES (42)")
        ex = DuckDBFileExecutor(db_path=duckdb_file, conn=mock_conn)
        result = ex.execute("SELECT x FROM t", [])
        assert result.success is True
        assert result.rows[0][0] == 42
