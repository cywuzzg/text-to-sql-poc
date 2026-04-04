from unittest.mock import MagicMock

import duckdb
import pandas as pd
import pytest

from text_to_sql.database.seed import generate_dataframes


@pytest.fixture
def in_memory_duckdb() -> duckdb.DuckDBPyConnection:
    """In-memory DuckDB connection with all four tables registered as views."""
    dfs = generate_dataframes()
    conn = duckdb.connect()
    for table_name, df in dfs.items():
        conn.register(table_name, df)
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
