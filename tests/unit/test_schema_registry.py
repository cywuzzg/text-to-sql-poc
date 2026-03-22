import pytest

from text_to_sql.database.schema_registry import (
    get_all_table_names,
    get_schema_detail_for_generation,
    get_schema_summary_for_routing,
    get_table_schema,
)


def test_all_four_tables_exist():
    names = get_all_table_names()
    assert set(names) == {"users", "products", "orders", "order_items"}


def test_get_table_schema_returns_correct_table():
    ts = get_table_schema("users")
    assert ts.name == "users"
    assert len(ts.columns) >= 4


def test_get_table_schema_unknown_raises_key_error():
    with pytest.raises(KeyError, match="Unknown table"):
        get_table_schema("nonexistent")


def test_get_schema_summary_contains_all_tables():
    summary = get_schema_summary_for_routing()
    for name in ["users", "products", "orders", "order_items"]:
        assert name in summary


def test_get_schema_summary_is_concise():
    summary = get_schema_summary_for_routing()
    # should not include full DDL
    assert "CREATE TABLE" not in summary


def test_get_schema_detail_for_single_table():
    detail = get_schema_detail_for_generation(["products"])
    assert "CREATE TABLE products" in detail
    assert "price" in detail
    assert "stock" in detail


def test_get_schema_detail_for_multiple_tables():
    detail = get_schema_detail_for_generation(["orders", "order_items"])
    assert "CREATE TABLE orders" in detail
    assert "CREATE TABLE order_items" in detail


def test_get_schema_detail_for_unknown_table_raises():
    with pytest.raises(KeyError):
        get_schema_detail_for_generation(["unknown_table"])


def test_table_schema_has_ddl():
    for name in get_all_table_names():
        ts = get_table_schema(name)
        assert "CREATE TABLE" in ts.ddl


def test_table_schema_has_example_queries():
    for name in get_all_table_names():
        ts = get_table_schema(name)
        assert len(ts.example_queries) > 0
