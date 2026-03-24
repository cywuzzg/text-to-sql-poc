"""Static SQL analyzer for classifying heavy queries."""
import re
from typing import List


class QueryClassifier:
    """Classify SQL queries as heavy or light using rule-based static analysis."""

    def is_heavy_query(self, sql: str) -> bool:
        """Return True if the SQL is classified as a heavy query."""
        return len(self.get_reason(sql)) > 0

    def get_reason(self, sql: str) -> List[str]:
        """Return list of reasons why the query is considered heavy.

        Useful for logging and debugging routing decisions.
        """
        reasons = []
        sql_upper = sql.upper()

        if self._has_group_by(sql_upper):
            reasons.append("GROUP BY")

        if self._has_having(sql_upper):
            reasons.append("HAVING")

        if self._has_window_function(sql_upper):
            reasons.append("WINDOW FUNCTION")

        if self._has_cross_join(sql_upper):
            reasons.append("CROSS JOIN")

        if self._has_aggregate_function(sql_upper):
            reasons.append("AGGREGATE FUNCTION")

        if self._is_full_table_scan_risk(sql_upper):
            reasons.append("NO LIMIT AND NO WHERE")

        if self._has_deep_nested_subquery(sql):
            reasons.append("NESTED SUBQUERY")

        return reasons

    def _has_group_by(self, sql_upper: str) -> bool:
        return bool(re.search(r'\bGROUP\s+BY\b', sql_upper))

    def _has_having(self, sql_upper: str) -> bool:
        return bool(re.search(r'\bHAVING\b', sql_upper))

    def _has_window_function(self, sql_upper: str) -> bool:
        return bool(re.search(r'\bOVER\s*\(', sql_upper))

    def _has_cross_join(self, sql_upper: str) -> bool:
        return bool(re.search(r'\bCROSS\s+JOIN\b', sql_upper))

    def _has_aggregate_function(self, sql_upper: str) -> bool:
        return bool(re.search(r'\b(COUNT|SUM|AVG|MAX|MIN)\s*\(', sql_upper))

    def _is_full_table_scan_risk(self, sql_upper: str) -> bool:
        """True if no LIMIT and no WHERE clause (full table scan risk)."""
        has_limit = bool(re.search(r'\bLIMIT\b', sql_upper))
        has_where = bool(re.search(r'\bWHERE\b', sql_upper))
        return not has_limit and not has_where

    def _has_deep_nested_subquery(self, sql: str) -> bool:
        """True if there are 2 or more levels of nested SELECT (3+ total SELECTs)."""
        sql_upper = sql.upper()
        # Tokenize to track paren depth and SELECT positions
        tokens = re.split(r'(\(|\)|SELECT)', sql_upper)
        depth = 0
        select_depths = []

        for token in tokens:
            if token == '(':
                depth += 1
            elif token == ')':
                depth -= 1
            elif token == 'SELECT':
                select_depths.append(depth)

        # Count SELECTs that appear inside parentheses (nested)
        nested_count = sum(1 for d in select_depths if d > 0)
        return nested_count >= 2
