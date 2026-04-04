"""Unit tests for QueryRouter."""
from unittest.mock import MagicMock

import pytest

from text_to_sql.models.response import ExecutionResult
from text_to_sql.routing.query_router import QueryRouter


def _make_execution_result(**kwargs) -> ExecutionResult:
    defaults = dict(success=True, columns=["id"], rows=[[1]], row_count=1)
    defaults.update(kwargs)
    return ExecutionResult(**defaults)


@pytest.fixture
def mock_duckdb_executor():
    executor = MagicMock()
    executor.execute.return_value = _make_execution_result()
    return executor


@pytest.fixture
def router(mock_duckdb_executor):
    return QueryRouter(duckdb_executor=mock_duckdb_executor)


class TestRoute:
    def test_heavy_query_routes_to_duckdb(self, router):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        assert router.route(sql) == "duckdb"

    def test_light_query_routes_to_duckdb_light(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        assert router.route(sql) == "duckdb_light"

    def test_aggregate_routes_to_duckdb(self, router):
        sql = "SELECT SUM(amount) FROM orders WHERE user_id = 1"
        assert router.route(sql) == "duckdb"

    def test_window_function_routes_to_duckdb(self, router):
        sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) FROM orders WHERE id > 0"
        assert router.route(sql) == "duckdb"

    def test_full_table_scan_routes_to_duckdb(self, router):
        sql = "SELECT * FROM orders"
        assert router.route(sql) == "duckdb"

    def test_simple_select_routes_to_duckdb_light(self, router):
        sql = "SELECT id, status FROM orders WHERE id = 42"
        assert router.route(sql) == "duckdb_light"


class TestExecute:
    def test_always_calls_duckdb_executor(self, router, mock_duckdb_executor):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        router.execute(sql, table_refs=["users"])
        mock_duckdb_executor.execute.assert_called_once_with(sql, ["users"])

    def test_heavy_query_calls_duckdb_executor(self, router, mock_duckdb_executor):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        router.execute(sql, table_refs=["orders"])
        mock_duckdb_executor.execute.assert_called_once_with(sql, ["orders"])

    def test_result_contains_engine_key(self, router):
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert "engine" in result

    def test_result_contains_data_key(self, router):
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert "data" in result

    def test_result_contains_reasons_key(self, router):
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert "reasons" in result

    def test_light_query_engine_is_duckdb_light(self, router):
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert result["engine"] == "duckdb_light"

    def test_heavy_query_engine_is_duckdb(self, router):
        result = router.execute(
            "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", table_refs=["orders"]
        )
        assert result["engine"] == "duckdb"

    def test_light_query_reasons_is_empty(self, router):
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert result["reasons"] == []

    def test_heavy_query_reasons_contains_trigger(self, router):
        result = router.execute(
            "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id", table_refs=["orders"]
        )
        assert len(result["reasons"]) > 0
        assert "GROUP BY" in result["reasons"]

    def test_data_is_execution_result(self, router, mock_duckdb_executor):
        mock_duckdb_executor.execute.return_value = _make_execution_result(
            columns=["id", "name"], rows=[[1, "Alice"]], row_count=1
        )
        result = router.execute("SELECT * FROM users WHERE id = 1 LIMIT 5", table_refs=["users"])
        assert isinstance(result["data"], ExecutionResult)

    def test_csv_url_propagated_from_executor(self, router, mock_duckdb_executor):
        mock_duckdb_executor.execute.return_value = _make_execution_result(
            rows=[], row_count=100, csv_url="results/large.csv"
        )
        result = router.execute("SELECT * FROM orders", table_refs=["orders"])
        assert result["data"].csv_url == "results/large.csv"
