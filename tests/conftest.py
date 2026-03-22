import sqlite3
from unittest.mock import MagicMock

import pytest

from text_to_sql.database.schema import DDL_STATEMENTS
from text_to_sql.database.seed import seed


@pytest.fixture
def in_memory_db() -> sqlite3.Connection:
    """In-memory SQLite DB with schema and seed data, auto-closed after test."""
    conn = sqlite3.connect(":memory:")
    conn.execute("PRAGMA foreign_keys = ON")
    for stmt in DDL_STATEMENTS:
        conn.execute(stmt)
    conn.commit()
    seed(conn)
    yield conn
    conn.close()


@pytest.fixture
def mock_claude_client() -> MagicMock:
    """A MagicMock that mimics anthropic.Anthropic with a configurable messages.create response."""
    client = MagicMock()
    # Callers configure: client.messages.create.return_value = ...
    return client


@pytest.fixture
def sample_route_result():
    from text_to_sql.models.request import RouteResult

    return RouteResult(
        tables=["products"],
        confidence=0.95,
        reasoning="Query is about product stock levels.",
    )


@pytest.fixture
def sample_generate_result():
    from text_to_sql.models.request import GenerateResult

    return GenerateResult(
        sql="SELECT * FROM products WHERE stock < 10",
        explanation="Retrieve all products with fewer than 10 units in stock.",
    )
