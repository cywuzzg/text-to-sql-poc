"""Table Router: maps natural language query to relevant table names."""
import json
import logging

import anthropic

from text_to_sql.database.schema_registry import get_schema_summary_for_routing
from text_to_sql.models.request import RouteResult
from text_to_sql.router.prompts import ROUTER_SYSTEM_TEMPLATE, ROUTER_USER_TEMPLATE

logger = logging.getLogger(__name__)

_LOW_CONFIDENCE_THRESHOLD = 0.5


class RouteParseError(Exception):
    """Raised when the Claude response cannot be parsed into a RouteResult."""


class TableRouter:
    def __init__(self, claude_client: anthropic.Anthropic, model: str):
        self._client = claude_client
        self._model = model

    def route(self, query: str) -> RouteResult:
        if not query or not query.strip():
            raise ValueError("Query must not be empty")

        system_prompt = ROUTER_SYSTEM_TEMPLATE.format(
            schema_summary=get_schema_summary_for_routing()
        )
        user_message = ROUTER_USER_TEMPLATE.format(user_query=query.strip())

        response = self._client.messages.create(
            model=self._model,
            max_tokens=256,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        raw_text = response.content[0].text.strip()
        return self._parse_response(raw_text)

    def _parse_response(self, raw_text: str) -> RouteResult:
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise RouteParseError(
                f"Cannot parse JSON from Claude response: {raw_text!r}"
            ) from exc

        if "tables" not in data:
            raise RouteParseError(
                f"Missing 'tables' field in Claude response: {data}"
            )

        result = RouteResult(
            tables=data["tables"],
            confidence=data.get("confidence", 1.0),
            reasoning=data.get("reasoning", ""),
        )

        if result.confidence < _LOW_CONFIDENCE_THRESHOLD:
            logger.warning(
                "Low routing confidence (%.2f) for tables %s",
                result.confidence,
                result.tables,
            )

        return result
