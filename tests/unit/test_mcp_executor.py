"""Unit tests for Executor layer using DirectSQLiteExecutor (no MCP subprocess)."""
import pytest

from text_to_sql.executor.mcp_executor import DirectSQLiteExecutor


@pytest.fixture
def executor(in_memory_db):
    return DirectSQLiteExecutor(conn=in_memory_db)


class TestDirectSQLiteExecutor:
    def test_select_all_users(self, executor):
        result = executor.execute("SELECT * FROM users")
        assert result.success is True
        assert result.row_count == 20
        assert "username" in result.columns

    def test_select_with_filter(self, executor):
        result = executor.execute("SELECT * FROM products WHERE stock < 100")
        assert result.success is True
        assert result.row_count > 0
        for row in result.rows:
            stock_idx = result.columns.index("stock")
            assert row[stock_idx] < 100

    def test_aggregate_query(self, executor):
        result = executor.execute("SELECT COUNT(*) as cnt FROM orders")
        assert result.success is True
        assert result.row_count == 1
        assert result.rows[0][0] == 60

    def test_join_query(self, executor):
        result = executor.execute(
            "SELECT u.username, COUNT(o.order_id) as order_count "
            "FROM users u LEFT JOIN orders o ON u.user_id = o.user_id "
            "GROUP BY u.user_id"
        )
        assert result.success is True
        assert "username" in result.columns
        assert "order_count" in result.columns

    def test_empty_result_set(self, executor):
        result = executor.execute(
            "SELECT * FROM products WHERE price > 9999999"
        )
        assert result.success is True
        assert result.row_count == 0
        assert result.rows == []

    def test_syntax_error_returns_failure(self, executor):
        result = executor.execute("SELECT ??? FROM users")
        assert result.success is False
        assert result.error is not None
        assert result.row_count == 0

    def test_nonexistent_table_returns_failure(self, executor):
        result = executor.execute("SELECT * FROM nonexistent_table")
        assert result.success is False
        assert result.error is not None

    def test_columns_returned_correctly(self, executor):
        result = executor.execute("SELECT username, region FROM users LIMIT 1")
        assert result.columns == ["username", "region"]
        assert len(result.rows[0]) == 2

    def test_order_by_query(self, executor):
        result = executor.execute(
            "SELECT name, price FROM products ORDER BY price DESC LIMIT 3"
        )
        assert result.success is True
        assert result.row_count == 3
        prices = [row[1] for row in result.rows]
        assert prices == sorted(prices, reverse=True)
