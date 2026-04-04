"""SQL Generator: converts natural language + schema context into a SQL SELECT statement."""
import json
import logging
import re

import anthropic

from text_to_sql.generator.prompts import GENERATOR_SYSTEM_TEMPLATE, GENERATOR_USER_TEMPLATE
from text_to_sql.models.request import GenerateResult

logger = logging.getLogger(__name__)

_UNSAFE_KEYWORDS = ("INSERT", "UPDATE", "DELETE", "DROP", "CREATE", "ALTER", "TRUNCATE", "REPLACE")
# Pre-compile word-boundary patterns so 'created_at' is not matched by 'CREATE'.
_UNSAFE_PATTERNS = [re.compile(rf"\b{kw}\b") for kw in _UNSAFE_KEYWORDS]


class UnsafeSQLError(Exception):
    """Raised when the generated SQL is not a SELECT statement."""


class SQLGenerator:
    def __init__(self, claude_client: anthropic.Anthropic, model: str):
        self._client = claude_client
        self._model = model

    def generate(self, query: str, schema_context: str) -> GenerateResult:
        system_prompt = GENERATOR_SYSTEM_TEMPLATE.format(schema_context=schema_context)
        user_message = GENERATOR_USER_TEMPLATE.format(user_query=query.strip())

        response = self._client.messages.create(
            model=self._model,
            max_tokens=512,
            system=system_prompt,
            messages=[{"role": "user", "content": user_message}],
        )

        raw_text = response.content[0].text.strip()
        return self._parse_response(raw_text)

    @staticmethod
    def _strip_markdown(text: str) -> str:
        if text.startswith("```"):
            text = text.split("\n", 1)[-1]
            text = text.rsplit("```", 1)[0]
        return text.strip()

    def _parse_response(self, raw_text: str) -> GenerateResult:
        raw_text = self._strip_markdown(raw_text)
        try:
            data = json.loads(raw_text)
        except json.JSONDecodeError as exc:
            raise ValueError(
                f"Cannot parse JSON from Claude response: {raw_text!r}"
            ) from exc

        sql: str = data.get("sql", "").strip()
        explanation: str = data.get("explanation", "")

        self._validate_sql(sql)

        return GenerateResult(sql=sql, explanation=explanation)

    @staticmethod
    def _validate_sql(sql: str) -> None:
        upper = sql.strip().upper()
        if not upper.startswith("SELECT"):
            raise UnsafeSQLError(
                f"Only SELECT statements are allowed. Got: {sql[:60]!r}"
            )
        for pattern, keyword in zip(_UNSAFE_PATTERNS, _UNSAFE_KEYWORDS):
            if pattern.search(upper):
                raise UnsafeSQLError(
                    f"SQL contains unsafe keyword '{keyword}': {sql[:60]!r}"
                )
