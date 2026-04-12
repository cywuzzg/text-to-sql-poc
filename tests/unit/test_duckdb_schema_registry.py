"""Unit tests for duckdb_schema_registry — dynamic schema loading from DuckDB + YAML."""
from pathlib import Path

import duckdb
import pytest

from text_to_sql.database.schema import DDL_STATEMENTS
from text_to_sql.database.duckdb_schema_registry import (
    build_registry,
    get_all_table_names,
    get_schema_detail_for_generation,
    get_schema_summary_for_routing,
    get_table_schema,
)

_METADATA_PATH = Path(__file__).parent.parent.parent / "src/text_to_sql/database/schema_metadata.yaml"


@pytest.fixture
def duckdb_conn_with_schema():
    """In-memory DuckDB with the four real tables created (empty)."""
    conn = duckdb.connect()
    for stmt in DDL_STATEMENTS:
        conn.execute(stmt)
    yield conn
    conn.close()


@pytest.fixture
def registry(duckdb_conn_with_schema):
    return build_registry(duckdb_conn_with_schema, _METADATA_PATH)


class TestBuildRegistry:
    def test_all_four_tables_exist(self, registry):
        assert set(registry.keys()) == {"users", "products", "orders", "order_items"}

    def test_column_names_from_duckdb(self, registry):
        col_names = [c.name for c in registry["users"].columns]
        assert "user_id" in col_names
        assert "username" in col_names
        assert "email" in col_names
        assert "created_at" in col_names
        assert "region" in col_names

    def test_column_types_from_duckdb(self, registry):
        """Types must be DuckDB native strings, not hardcoded SQLite-style."""
        type_map = {c.name: c.type for c in registry["users"].columns}
        assert type_map["user_id"].upper() == "INTEGER"

    def test_column_description_from_yaml(self, registry):
        col_map = {c.name: c.description for c in registry["users"].columns}
        assert col_map["user_id"] == "主鍵，自動遞增"
        assert col_map["region"] == "所屬地區：north / south / east / west"

    def test_column_missing_from_yaml_gets_empty_description(self, duckdb_conn_with_schema):
        """Column in DuckDB but not in YAML → description=''."""
        # Add an extra column not in YAML by creating a fresh conn with extra table
        conn = duckdb.connect()
        conn.execute("CREATE TABLE extra_table (col_a INTEGER, col_b VARCHAR)")
        # Use a YAML that has no 'extra_table' key
        registry = build_registry(conn, _METADATA_PATH)
        # extra_table should not appear (no YAML entry), but we can verify the fallback:
        # Create inline metadata with only partial column coverage
        import tempfile, yaml
        meta = {"tables": {"extra_table": {"description": "test", "example_queries": [], "columns": {}}}}
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(meta, f)
            tmp_path = Path(f.name)
        reg2 = build_registry(conn, tmp_path)
        col_map = {c.name: c.description for c in reg2["extra_table"].columns}
        assert col_map["col_a"] == ""
        assert col_map["col_b"] == ""
        conn.close()

    def test_unknown_column_in_yaml_is_ignored(self, duckdb_conn_with_schema):
        """YAML has a column name not in DuckDB → no KeyError."""
        import tempfile, yaml
        meta = {
            "tables": {
                "users": {
                    "description": "test",
                    "example_queries": [],
                    "columns": {
                        "user_id": "主鍵",
                        "nonexistent_col": "這欄位不存在",
                    },
                }
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(meta, f, allow_unicode=True)
            tmp_path = Path(f.name)
        registry = build_registry(duckdb_conn_with_schema, tmp_path)
        col_names = [c.name for c in registry["users"].columns]
        assert "nonexistent_col" not in col_names

    def test_unknown_table_in_yaml_is_ignored(self, duckdb_conn_with_schema):
        """YAML has a table not in DuckDB → not added to registry."""
        import tempfile, yaml
        meta = {
            "tables": {
                "users": {"description": "test", "example_queries": [], "columns": {}},
                "ghost_table": {"description": "not in db", "example_queries": [], "columns": {}},
            }
        }
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(meta, f, allow_unicode=True)
            tmp_path = Path(f.name)
        registry = build_registry(duckdb_conn_with_schema, tmp_path)
        assert "ghost_table" not in registry

    def test_table_description_from_yaml(self, registry):
        assert registry["users"].description == "電商平台用戶資料，包含帳號、Email、所屬地區"
        assert registry["orders"].description == "訂單主表，記錄每筆訂單的用戶、狀態、總金額"

    def test_example_queries_from_yaml(self, registry):
        assert "查詢某地區所有用戶" in registry["users"].example_queries
        assert "銷售量最高的商品" in registry["order_items"].example_queries

    def test_ddl_from_schema_py(self, registry):
        assert "CREATE TABLE" in registry["users"].ddl
        assert "CREATE TABLE" in registry["products"].ddl

    def test_table_schema_missing_from_yaml_still_registered(self, duckdb_conn_with_schema):
        """Table in DuckDB but absent from YAML → still in registry with empty metadata."""
        import tempfile, yaml
        meta = {"tables": {}}  # empty YAML
        with tempfile.NamedTemporaryFile(mode="w", suffix=".yaml", delete=False) as f:
            yaml.dump(meta, f)
            tmp_path = Path(f.name)
        registry = build_registry(duckdb_conn_with_schema, tmp_path)
        # All DuckDB tables should still appear
        assert "users" in registry
        assert registry["users"].description == ""
        assert registry["users"].example_queries == []


class TestPublicAPI:
    def test_get_table_schema_raises_for_unknown_table(self, monkeypatch, registry):
        import text_to_sql.database.duckdb_schema_registry as mod
        monkeypatch.setattr(mod, "_registry_cache", registry)
        with pytest.raises(KeyError, match="ghost"):
            get_table_schema("ghost")

    def test_get_all_table_names_returns_list(self, monkeypatch, registry):
        import text_to_sql.database.duckdb_schema_registry as mod
        monkeypatch.setattr(mod, "_registry_cache", registry)
        names = get_all_table_names()
        assert set(names) == {"users", "products", "orders", "order_items"}

    def test_get_schema_summary_for_routing_includes_all_tables(self, monkeypatch, registry):
        import text_to_sql.database.duckdb_schema_registry as mod
        monkeypatch.setattr(mod, "_registry_cache", registry)
        summary = get_schema_summary_for_routing()
        for table in ["users", "products", "orders", "order_items"]:
            assert table in summary

    def test_get_schema_detail_for_generation_includes_ddl(self, monkeypatch, registry):
        import text_to_sql.database.duckdb_schema_registry as mod
        monkeypatch.setattr(mod, "_registry_cache", registry)
        detail = get_schema_detail_for_generation(["users", "orders"])
        assert "CREATE TABLE" in detail
        assert "users" in detail
        assert "orders" in detail

    def test_get_schema_detail_for_generation_includes_column_descriptions(self, monkeypatch, registry):
        import text_to_sql.database.duckdb_schema_registry as mod
        monkeypatch.setattr(mod, "_registry_cache", registry)
        detail = get_schema_detail_for_generation(["users"])
        assert "主鍵，自動遞增" in detail
