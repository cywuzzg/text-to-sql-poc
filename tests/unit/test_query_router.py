"""Unit tests for QueryRouter."""
from typing import Literal
from unittest.mock import MagicMock

import pandas as pd
import pytest

from text_to_sql.routing.query_router import QueryRouter


@pytest.fixture
def mock_main_db_executor():
    executor = MagicMock()
    mock_result = MagicMock()
    mock_result.success = True
    mock_result.columns = ["id", "name"]
    mock_result.rows = [[1, "Alice"]]
    mock_result.row_count = 1
    executor.execute.return_value = mock_result
    return executor


@pytest.fixture
def mock_duckdb_executor():
    executor = MagicMock()
    executor.execute.return_value = pd.DataFrame({"user_id": [10], "total": [300.0]})
    return executor


@pytest.fixture
def router(mock_main_db_executor, mock_duckdb_executor):
    return QueryRouter(
        main_db_executor=mock_main_db_executor,
        duckdb_executor=mock_duckdb_executor,
    )


class TestRoute:
    def test_heavy_query_routes_to_duckdb(self, router):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        assert router.route(sql) == "duckdb"

    def test_light_query_routes_to_main_db(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        assert router.route(sql) == "main_db"

    def test_aggregate_routes_to_duckdb(self, router):
        sql = "SELECT SUM(amount) FROM orders WHERE user_id = 1"
        assert router.route(sql) == "duckdb"

    def test_window_function_routes_to_duckdb(self, router):
        sql = "SELECT id, ROW_NUMBER() OVER (ORDER BY created_at) FROM orders WHERE id > 0"
        assert router.route(sql) == "duckdb"

    def test_full_table_scan_routes_to_duckdb(self, router):
        sql = "SELECT * FROM orders"
        assert router.route(sql) == "duckdb"

    def test_simple_select_routes_to_main_db(self, router):
        sql = "SELECT id, status FROM orders WHERE id = 42"
        assert router.route(sql) == "main_db"

    def test_returns_literal_type_main_db(self, router):
        result = router.route("SELECT * FROM users WHERE id = 1 LIMIT 5")
        assert result in ("main_db", "duckdb")

    def test_returns_literal_type_duckdb(self, router):
        result = router.route("SELECT COUNT(*) FROM orders")
        assert result in ("main_db", "duckdb")


class TestExecute:
    def test_light_query_uses_main_db_executor(
        self, router, mock_main_db_executor, mock_duckdb_executor
    ):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        router.execute(sql, table_refs=["users"])

        mock_main_db_executor.execute.assert_called_once_with(sql)
        mock_duckdb_executor.execute.assert_not_called()

    def test_heavy_query_uses_duckdb_executor(
        self, router, mock_main_db_executor, mock_duckdb_executor
    ):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        router.execute(sql, table_refs=["orders"])

        mock_duckdb_executor.execute.assert_called_once_with(sql, ["orders"])
        mock_main_db_executor.execute.assert_not_called()

    def test_result_contains_engine_key(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        assert "engine" in result

    def test_result_contains_data_key(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        assert "data" in result

    def test_result_contains_reasons_key(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        assert "reasons" in result

    def test_light_query_result_engine_is_main_db(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        assert result["engine"] == "main_db"

    def test_heavy_query_result_engine_is_duckdb(self, router):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        result = router.execute(sql, table_refs=["orders"])
        assert result["engine"] == "duckdb"

    def test_light_query_result_reasons_is_empty(self, router):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        assert result["reasons"] == []

    def test_heavy_query_result_reasons_contains_trigger(self, router):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        result = router.execute(sql, table_refs=["orders"])
        assert len(result["reasons"]) > 0
        assert "GROUP BY" in result["reasons"]

    def test_main_db_result_data_type(self, router, mock_main_db_executor):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        result = router.execute(sql, table_refs=["users"])
        # Data from main_db executor is ExecutionResult
        assert result["data"] == mock_main_db_executor.execute.return_value

    def test_duckdb_result_data_is_dataframe(self, router):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        result = router.execute(sql, table_refs=["orders"])
        assert isinstance(result["data"], pd.DataFrame)
