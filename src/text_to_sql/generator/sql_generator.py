"""SQL Generator: converts natural language + schema context into a SQL SELECT statement."""
import logging

import anthropic
import sqlglot
from sqlglot import exp

from text_to_sql.generator.prompts import GENERATOR_SYSTEM_TEMPLATE, GENERATOR_USER_TEMPLATE
from text_to_sql.models.request import GenerateResult

logger = logging.getLogger(__name__)

_UNSAFE_NODE_TYPES = (
    exp.Insert,
    exp.Update,
    exp.Delete,
    exp.Drop,
    exp.Create,
    exp.Alter,
    exp.TruncateTable,
)

_GENERATE_TOOL = {
    "name": "output_sql",
    "description": "Return the generated DuckDB SQL and a one-sentence explanation.",
    "input_schema": {
        "type": "object",
        "properties": {
            "sql": {"type": "string"},
            "explanation": {"type": "string"},
        },
        "required": ["sql", "explanation"],
    },
}


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
            tools=[_GENERATE_TOOL],
            tool_choice={"type": "tool", "name": "output_sql"},
        )

        data: dict = response.content[0].input
        sql: str = data.get("sql", "").strip()
        explanation: str = data.get("explanation", "")

        self._validate_sql(sql)

        return GenerateResult(sql=sql, explanation=explanation)

    @staticmethod
    def _validate_sql(sql: str) -> None:
        try:
            tree = sqlglot.parse_one(sql.strip(), dialect="duckdb")
        except sqlglot.errors.ParseError as exc:
            raise UnsafeSQLError(f"Invalid SQL: {exc}") from exc

        if not isinstance(tree, exp.Select):
            raise UnsafeSQLError(
                f"Only SELECT statements are allowed. Got: {sql[:60]!r}"
            )

        for node in tree.walk():
            if isinstance(node, _UNSAFE_NODE_TYPES):
                raise UnsafeSQLError(
                    f"Unsafe operation '{type(node).__name__}' found in SQL: {sql[:60]!r}"
                )
