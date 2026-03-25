"""Unit tests for TableRouter (Claude API is mocked)."""
import json
from unittest.mock import MagicMock

import pytest

from text_to_sql.models.request import RouteResult
from text_to_sql.router.table_router import RouteParseError, TableRouter


def _make_router(mock_client: MagicMock) -> TableRouter:
    return TableRouter(claude_client=mock_client, model="claude-haiku-4-5-20251001")


def _set_response(mock_client: MagicMock, payload: dict) -> None:
    """Configure the mock to return a JSON string as the assistant message."""
    message = MagicMock()
    message.content = [MagicMock(text=json.dumps(payload))]
    mock_client.messages.create.return_value = message


class TestTableRouterRoute:
    def test_single_table_product_query(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"tables": ["products"], "confidence": 0.95, "reasoning": "stock question"},
        )
        router = _make_router(mock_claude_client)
        result = router.route("庫存不足的商品有哪些？")

        assert isinstance(result, RouteResult)
        assert result.tables == ["products"]
        assert result.confidence == 0.95

    def test_multi_table_sales_query(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {
                "tables": ["products", "orders", "order_items"],
                "confidence": 0.9,
                "reasoning": "sales analysis needs join",
            },
        )
        router = _make_router(mock_claude_client)
        result = router.route("最近 30 天銷售額最高的商品？")

        assert set(result.tables) == {"products", "orders", "order_items"}

    def test_two_table_region_query(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"tables": ["users", "orders"], "confidence": 0.88, "reasoning": "region join"},
        )
        router = _make_router(mock_claude_client)
        result = router.route("哪個地區的用戶消費最多？")

        assert "users" in result.tables
        assert "orders" in result.tables

    def test_invalid_json_raises_route_parse_error(self, mock_claude_client):
        message = MagicMock()
        message.content = [MagicMock(text="not valid json at all")]
        mock_claude_client.messages.create.return_value = message

        router = _make_router(mock_claude_client)
        with pytest.raises(RouteParseError):
            router.route("some query")

    def test_missing_tables_field_raises_route_parse_error(self, mock_claude_client):
        _set_response(mock_claude_client, {"confidence": 0.9, "reasoning": "no tables key"})
        router = _make_router(mock_claude_client)
        with pytest.raises(RouteParseError):
            router.route("some query")

    def test_empty_query_raises_value_error(self, mock_claude_client):
        router = _make_router(mock_claude_client)
        with pytest.raises(ValueError, match="empty"):
            router.route("")

    def test_whitespace_query_raises_value_error(self, mock_claude_client):
        router = _make_router(mock_claude_client)
        with pytest.raises(ValueError, match="empty"):
            router.route("   ")

    def test_low_confidence_still_returns_result(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"tables": ["users"], "confidence": 0.3, "reasoning": "uncertain"},
        )
        router = _make_router(mock_claude_client)
        result = router.route("something ambiguous")
        assert result.confidence == 0.3

    def test_claude_api_called_with_correct_model(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"tables": ["products"], "confidence": 0.9, "reasoning": "ok"},
        )
        router = _make_router(mock_claude_client)
        router.route("test query")

        call_kwargs = mock_claude_client.messages.create.call_args
        assert call_kwargs.kwargs["model"] == "claude-haiku-4-5-20251001"

    def test_markdown_wrapped_json_is_parsed(self, mock_claude_client):
        payload = {"tables": ["products"], "confidence": 0.95, "reasoning": "stock question"}
        message = MagicMock()
        message.content = [MagicMock(text=f"```json\n{json.dumps(payload)}\n```")]
        mock_claude_client.messages.create.return_value = message

        router = _make_router(mock_claude_client)
        result = router.route("庫存不足的商品有哪些？")
        assert result.tables == ["products"]

    def test_system_prompt_contains_table_names(self, mock_claude_client):
        _set_response(
            mock_claude_client,
            {"tables": ["orders"], "confidence": 0.85, "reasoning": "ok"},
        )
        router = _make_router(mock_claude_client)
        router.route("show me orders")

        call_kwargs = mock_claude_client.messages.create.call_args.kwargs
        system_prompt = call_kwargs["system"]
        for table in ["users", "products", "orders", "order_items"]:
            assert table in system_prompt
