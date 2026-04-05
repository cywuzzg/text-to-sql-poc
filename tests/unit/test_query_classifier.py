"""Unit tests for QueryClassifier."""
import pytest

from text_to_sql.routing.query_classifier import QueryClassifier


@pytest.fixture
def classifier():
    return QueryClassifier()


class TestIsHeavyQuery:
    def test_simple_select_with_where_and_limit_is_not_heavy(self, classifier):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        assert classifier.is_heavy_query(sql) is False

    def test_simple_select_with_where_is_not_heavy(self, classifier):
        sql = "SELECT id, name FROM users WHERE active = 1"
        assert classifier.is_heavy_query(sql) is False

    def test_simple_select_with_limit_is_not_heavy(self, classifier):
        sql = "SELECT * FROM orders LIMIT 10"
        assert classifier.is_heavy_query(sql) is False

    def test_group_by_is_heavy(self, classifier):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        assert classifier.is_heavy_query(sql) is True

    def test_having_is_heavy(self, classifier):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5"
        assert classifier.is_heavy_query(sql) is True

    def test_window_function_is_heavy(self, classifier):
        sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders"
        assert classifier.is_heavy_query(sql) is True

    def test_cross_join_is_heavy(self, classifier):
        sql = "SELECT * FROM products CROSS JOIN orders"
        assert classifier.is_heavy_query(sql) is True

    def test_count_aggregate_is_heavy(self, classifier):
        sql = "SELECT COUNT(*) FROM orders"
        assert classifier.is_heavy_query(sql) is True

    def test_sum_aggregate_is_heavy(self, classifier):
        sql = "SELECT SUM(price) FROM products WHERE active = 1"
        assert classifier.is_heavy_query(sql) is True

    def test_avg_aggregate_is_heavy(self, classifier):
        sql = "SELECT AVG(price) FROM products WHERE active = 1"
        assert classifier.is_heavy_query(sql) is True

    def test_max_aggregate_is_heavy(self, classifier):
        sql = "SELECT MAX(price) FROM products WHERE category = 'A'"
        assert classifier.is_heavy_query(sql) is True

    def test_min_aggregate_is_heavy(self, classifier):
        sql = "SELECT MIN(price) FROM products WHERE category = 'A'"
        assert classifier.is_heavy_query(sql) is True

    def test_no_limit_no_where_is_heavy(self, classifier):
        sql = "SELECT * FROM orders"
        assert classifier.is_heavy_query(sql) is True

    def test_no_limit_no_where_select_columns_is_heavy(self, classifier):
        sql = "SELECT id, user_id, amount FROM orders"
        assert classifier.is_heavy_query(sql) is True

    def test_with_limit_no_where_is_not_heavy(self, classifier):
        sql = "SELECT * FROM orders LIMIT 10"
        assert classifier.is_heavy_query(sql) is False

    def test_with_where_no_limit_is_not_heavy(self, classifier):
        sql = "SELECT * FROM orders WHERE user_id = 1"
        assert classifier.is_heavy_query(sql) is False

    def test_deep_nested_subquery_is_heavy(self, classifier):
        # 3 levels of SELECT (2 nested)
        sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM orders WHERE id = 1) t1) t2"
        assert classifier.is_heavy_query(sql) is True

    def test_single_nested_subquery_not_heavy_by_nesting(self, classifier):
        # Only 1 nested level, plus WHERE and LIMIT to avoid other triggers
        sql = "SELECT id FROM (SELECT id FROM users WHERE active = 1) t1 LIMIT 5"
        assert classifier.is_heavy_query(sql) is False

    def test_case_insensitive_group_by(self, classifier):
        sql = "select user_id, count(*) from orders group by user_id"
        assert classifier.is_heavy_query(sql) is True

    def test_case_insensitive_window_function(self, classifier):
        sql = "select id, row_number() over (order by created_at) from orders where id=1"
        assert classifier.is_heavy_query(sql) is True


class TestGetReason:
    def test_no_heavy_query_returns_empty(self, classifier):
        sql = "SELECT * FROM users WHERE id = 1 LIMIT 5"
        assert classifier.get_reason(sql) == []

    def test_group_by_reason(self, classifier):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        reasons = classifier.get_reason(sql)
        assert "GROUP BY" in reasons

    def test_having_reason(self, classifier):
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id HAVING COUNT(*) > 5"
        reasons = classifier.get_reason(sql)
        assert "HAVING" in reasons

    def test_window_function_reason(self, classifier):
        sql = "SELECT id, ROW_NUMBER() OVER (PARTITION BY user_id ORDER BY created_at) FROM orders"
        reasons = classifier.get_reason(sql)
        assert "WINDOW FUNCTION" in reasons

    def test_cross_join_reason(self, classifier):
        sql = "SELECT * FROM products CROSS JOIN orders"
        reasons = classifier.get_reason(sql)
        assert "CROSS JOIN" in reasons

    def test_count_aggregate_reason(self, classifier):
        sql = "SELECT COUNT(*) FROM orders"
        reasons = classifier.get_reason(sql)
        assert "AGGREGATE FUNCTION" in reasons

    def test_sum_aggregate_reason(self, classifier):
        sql = "SELECT SUM(price) FROM products WHERE active = 1"
        reasons = classifier.get_reason(sql)
        assert "AGGREGATE FUNCTION" in reasons

    def test_no_limit_no_where_reason(self, classifier):
        sql = "SELECT * FROM orders"
        reasons = classifier.get_reason(sql)
        assert "NO LIMIT AND NO WHERE" in reasons

    def test_nested_subquery_reason(self, classifier):
        sql = "SELECT * FROM (SELECT * FROM (SELECT * FROM orders WHERE id=1) t1) t2"
        reasons = classifier.get_reason(sql)
        assert "NESTED SUBQUERY" in reasons

    def test_multiple_reasons_returned(self, classifier):
        # GROUP BY + aggregate + no WHERE (though GROUP BY implies aggregate)
        sql = "SELECT user_id, COUNT(*) FROM orders GROUP BY user_id"
        reasons = classifier.get_reason(sql)
        assert len(reasons) >= 2
        assert "GROUP BY" in reasons
        assert "AGGREGATE FUNCTION" in reasons

    def test_returns_list_type(self, classifier):
        sql = "SELECT * FROM users WHERE id = 1"
        result = classifier.get_reason(sql)
        assert isinstance(result, list)


class TestStringLiteralEdgeCases:
    """Keywords inside string literals must not influence classification."""

    def test_group_by_in_string_literal_not_heavy(self, classifier):
        sql = "SELECT * FROM orders WHERE note = 'this GROUP BY user was applied' LIMIT 10"
        assert classifier.is_heavy_query(sql) is False

    def test_having_in_string_literal_not_heavy(self, classifier):
        sql = "SELECT * FROM users WHERE label = 'HAVING clause example' LIMIT 10"
        assert classifier.is_heavy_query(sql) is False

    def test_count_call_in_string_literal_not_heavy(self, classifier):
        sql = "SELECT * FROM products WHERE description = 'COUNT(*) is expensive' LIMIT 10"
        assert classifier.is_heavy_query(sql) is False

    def test_window_over_in_string_literal_not_heavy(self, classifier):
        sql = "SELECT * FROM orders WHERE note = 'use OVER (PARTITION BY x)' LIMIT 10"
        assert classifier.is_heavy_query(sql) is False
