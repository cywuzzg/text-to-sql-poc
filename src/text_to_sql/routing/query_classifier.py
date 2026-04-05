"""Static SQL analyzer for classifying heavy queries."""
from typing import List

import sqlglot
from sqlglot import exp


class QueryClassifier:
    """Classify SQL queries as heavy or light using AST-based static analysis."""

    def is_heavy_query(self, sql: str) -> bool:
        """Return True if the SQL is classified as a heavy query."""
        return len(self.get_reason(sql)) > 0

    def get_reason(self, sql: str) -> List[str]:
        """Return list of reasons why the query is considered heavy.

        Useful for logging and debugging routing decisions.
        """
        try:
            tree = sqlglot.parse_one(sql, dialect="duckdb")
        except sqlglot.errors.ParseError:
            return ["INVALID SQL"]

        reasons = []

        if tree.find(exp.Group):
            reasons.append("GROUP BY")

        if tree.find(exp.Having):
            reasons.append("HAVING")

        if tree.find(exp.Window):
            reasons.append("WINDOW FUNCTION")

        if any(
            j.args.get("kind", "").upper() == "CROSS"
            for j in tree.find_all(exp.Join)
        ):
            reasons.append("CROSS JOIN")

        if any(tree.find(cls) for cls in (exp.Count, exp.Sum, exp.Avg, exp.Max, exp.Min)):
            reasons.append("AGGREGATE FUNCTION")

        has_where = tree.find(exp.Where) is not None
        has_limit = tree.find(exp.Limit) is not None
        if not has_where and not has_limit:
            reasons.append("NO LIMIT AND NO WHERE")

        if self._has_deep_nested_subquery(tree):
            reasons.append("NESTED SUBQUERY")

        return reasons

    @staticmethod
    def _has_deep_nested_subquery(tree: exp.Expression) -> bool:
        """True if there are 2+ levels of nested subqueries in FROM clauses."""
        return len(list(tree.find_all(exp.Subquery))) >= 2
