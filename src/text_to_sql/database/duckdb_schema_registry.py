"""Dynamic schema registry that reads column metadata from a DuckDB file.

Column names and types are derived from DuckDB's information_schema.
Table/column descriptions and example_queries come from schema_metadata.yaml.
DDL strings are sourced from schema.py (same as the static registry).

Public API mirrors schema_registry.py:
  - get_all_table_names()
  - get_table_schema(table_name)
  - get_schema_summary_for_routing()
  - get_schema_detail_for_generation(tables)
"""
import re
from pathlib import Path
from typing import Dict, List, Optional

import duckdb
import yaml

from text_to_sql.database.schema import DDL_STATEMENTS
from text_to_sql.database.schema_registry import ColumnInfo, TableSchema

_METADATA_PATH = Path(__file__).parent / "schema_metadata.yaml"

_registry_cache: Optional[Dict[str, TableSchema]] = None


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _load_yaml_metadata(path: Path) -> dict:
    with open(path, encoding="utf-8") as f:
        return yaml.safe_load(f).get("tables", {})


def _query_duckdb_columns(conn) -> Dict[str, List[tuple]]:
    """Return {table_name: [(col_name, col_type), ...]} from information_schema."""
    rows = conn.execute("""
        SELECT table_name, column_name, data_type
        FROM information_schema.columns
        WHERE table_schema = 'main'
        ORDER BY table_name, ordinal_position
    """).fetchall()
    result: Dict[str, List[tuple]] = {}
    for table_name, col_name, col_type in rows:
        result.setdefault(table_name, []).append((col_name, col_type))
    return result


def _build_ddl_map() -> Dict[str, str]:
    """Parse DDL_STATEMENTS into {table_name: ddl_string}."""
    result = {}
    for stmt in DDL_STATEMENTS:
        m = re.search(r'CREATE TABLE IF NOT EXISTS\s+(\w+)', stmt, re.IGNORECASE)
        if m:
            result[m.group(1)] = stmt.strip()
    return result


# ---------------------------------------------------------------------------
# Public builder (testable without touching the global cache)
# ---------------------------------------------------------------------------

def build_registry(conn, metadata_path: Path = _METADATA_PATH) -> Dict[str, TableSchema]:
    """Build a schema registry from a DuckDB connection and a YAML metadata file.

    Args:
        conn: An open DuckDB connection.
        metadata_path: Path to the YAML sidecar with descriptions and example_queries.

    Returns:
        Dict mapping table name → TableSchema.
    """
    yaml_meta = _load_yaml_metadata(metadata_path)
    db_columns = _query_duckdb_columns(conn)
    ddl_map = _build_ddl_map()

    registry: Dict[str, TableSchema] = {}
    for table_name, cols in db_columns.items():
        table_meta = yaml_meta.get(table_name, {})
        col_descs = table_meta.get("columns", {})
        columns = [
            ColumnInfo(name=col_name, type=col_type, description=col_descs.get(col_name, ""))
            for col_name, col_type in cols
        ]
        registry[table_name] = TableSchema(
            name=table_name,
            description=table_meta.get("description", ""),
            columns=columns,
            ddl=ddl_map.get(table_name, ""),
            example_queries=table_meta.get("example_queries", []),
        )
    return registry


# ---------------------------------------------------------------------------
# Lazy module-level singleton (fails at call time, not import time)
# ---------------------------------------------------------------------------

def _get_registry() -> Dict[str, TableSchema]:
    global _registry_cache
    if _registry_cache is None:
        from text_to_sql.config import DUCKDB_PATH
        conn = duckdb.connect(DUCKDB_PATH, read_only=True)
        try:
            _registry_cache = build_registry(conn)
        finally:
            conn.close()
    return _registry_cache


# ---------------------------------------------------------------------------
# Public API (same interface as schema_registry.py)
# ---------------------------------------------------------------------------

def get_all_table_names() -> List[str]:
    return list(_get_registry().keys())


def get_table_schema(table_name: str) -> TableSchema:
    registry = _get_registry()
    if table_name not in registry:
        raise KeyError(f"Unknown table: '{table_name}'. Available: {list(registry.keys())}")
    return registry[table_name]


def get_schema_summary_for_routing() -> str:
    """Return a concise summary of all tables for the Table Router prompt."""
    registry = _get_registry()
    lines = ["可用資料表：\n"]
    for ts in registry.values():
        key_cols = ", ".join(c.name for c in ts.columns[:4])
        lines.append(f"- {ts.name}: {ts.description}（主要欄位：{key_cols}）")
    return "\n".join(lines)


def get_schema_detail_for_generation(tables: List[str]) -> str:
    """Return full DDL + column descriptions for the given tables."""
    parts = []
    for table_name in tables:
        ts = get_table_schema(table_name)
        col_desc = "\n".join(
            f"  - {c.name} ({c.type}): {c.description}" for c in ts.columns
        )
        parts.append(f"### {ts.name}\n{ts.ddl}\n\n欄位說明：\n{col_desc}")
    return "\n\n".join(parts)
