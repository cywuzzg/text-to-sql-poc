"""Unit tests for SQLGenerator (Claude API is mocked)."""
import json
from unittest.mock import MagicMock

import pytest

from text_to_sql.generator.sql_generator import SQLGenerator, UnsafeSQLError
from text_to_sql.models.request import GenerateResult


def _make_generator(mock_client: MagicMock) -> SQLGenerator:
    return SQLGenerator(claude_client=mock_client, model="claude-haiku-4-5-20251001")


def _set_response(mock_client: MagicMock, payload: dict) -> None:
    message = MagicMock()
    message.content = [MagicMock(text=json.dumps(payload))]
    mock_client.messages.create.return_value = message


SAMPLE_SCHEMA = "CREATE TABLE products (product_id INTEGER, name TEXT, price REAL, stock INTEGER)"


class TestSQLGeneratorGenerate:
    def test_basic_select_generation(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {
                "sql": "SELECT * FROM products WHERE stock < 10",
                "explanation": "查詢庫存不足 10 件的商品",
            },
        )
        gen = _make_generator(mock_claude_client)
        result = gen.generate("庫存不足的商品", SAMPLE_SCHEMA)

        assert isinstance(result, GenerateResult)
        assert result.sql.upper().startswith("SELECT")

    def test_aggregate_query_generation(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {
                "sql": "SELECT category, COUNT(*) as cnt FROM products GROUP BY category",
                "explanation": "各品類商品數量",
            },
        )
        gen = _make_generator(mock_claude_client)
        result = gen.generate("各品類有多少商品", SAMPLE_SCHEMA)
        assert "GROUP BY" in result.sql.upper()

    def test_unsafe_sql_raises_error(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "DROP TABLE products", "explanation": "dangerous"},
        )
        gen = _make_generator(mock_claude_client)
        with pytest.raises(UnsafeSQLError):
            gen.generate("some query", SAMPLE_SCHEMA)

    def test_delete_sql_raises_unsafe_error(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "DELETE FROM products WHERE 1=1", "explanation": "delete all"},
        )
        gen = _make_generator(mock_claude_client)
        with pytest.raises(UnsafeSQLError):
            gen.generate("some query", SAMPLE_SCHEMA)

    def test_insert_sql_raises_unsafe_error(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "INSERT INTO products VALUES (1,'x',1.0,10)", "explanation": "insert"},
        )
        gen = _make_generator(mock_claude_client)
        with pytest.raises(UnsafeSQLError):
            gen.generate("some query", SAMPLE_SCHEMA)

    def test_created_at_column_not_flagged_as_unsafe(self, mock_claude_client):
        """Regression: 'created_at' contains 'CREATE' as a substring — must not raise."""
        _set_response(
            mock_claude_client,
            {
                "sql": "SELECT MIN(created_at) AS earliest, MAX(created_at) AS latest FROM orders",
                "explanation": "earliest and latest order times",
            },
        )
        gen = _make_generator(mock_claude_client)
        result = gen.generate("最早和最晚的訂單時間", SAMPLE_SCHEMA)
        assert result.sql is not None

    def test_alter_keyword_as_standalone_is_blocked(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "ALTER TABLE products ADD COLUMN x INT", "explanation": "alter"},
        )
        gen = _make_generator(mock_claude_client)
        with pytest.raises(UnsafeSQLError):
            gen.generate("some query", SAMPLE_SCHEMA)

    def test_invalid_json_raises_value_error(self, mock_claude_client):
        message = MagicMock()
        message.content = [MagicMock(text="not json")]
        mock_claude_client.messages.create.return_value = message

        gen = _make_generator(mock_claude_client)
        with pytest.raises(ValueError):
            gen.generate("some query", SAMPLE_SCHEMA)

    def test_schema_context_included_in_prompt(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "SELECT 1", "explanation": "trivial"},
        )
        gen = _make_generator(mock_claude_client)
        gen.generate("test", SAMPLE_SCHEMA)

        call_kwargs = mock_claude_client.messages.create.call_args.kwargs
        # schema should appear in the system prompt
        system = call_kwargs["system"]
        assert "CREATE TABLE products" in system

    def test_explanation_returned(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "SELECT name FROM products", "explanation": "商品名稱列表"},
        )
        gen = _make_generator(mock_claude_client)
        result = gen.generate("列出商品名稱", SAMPLE_SCHEMA)
        assert result.explanation == "商品名稱列表"

    def test_markdown_wrapped_json_is_parsed(self, mock_claude_client):
        payload = {"sql": "SELECT * FROM products WHERE stock < 50", "explanation": "庫存不足"}
        message = MagicMock()
        message.content = [MagicMock(text=f"```json\n{json.dumps(payload)}\n```")]
        mock_claude_client.messages.create.return_value = message

        gen = _make_generator(mock_claude_client)
        result = gen.generate("庫存不足的商品", SAMPLE_SCHEMA)
        assert result.sql.upper().startswith("SELECT")

    def test_select_with_leading_whitespace_is_allowed(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"sql": "  SELECT * FROM products  ", "explanation": "ok"},
        )
        gen = _make_generator(mock_claude_client)
        result = gen.generate("all products", SAMPLE_SCHEMA)
        assert result.sql.strip().upper().startswith("SELECT")
