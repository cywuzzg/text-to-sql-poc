"""Query router: classify SQL and dispatch to DuckDB (all queries use DuckDB)."""
import logging
from typing import Dict, List, Literal

from text_to_sql.models.response import ExecutionResult
from text_to_sql.routing.query_classifier import QueryClassifier

logger = logging.getLogger(__name__)


class QueryRouter:
    """Route SQL queries to DuckDB, distinguishing light from heavy for observability.

    Both light and heavy queries are executed by the same DuckDBExecutor.
    The classification is preserved in the result so callers can observe which
    path was taken and adjust behaviour (e.g., caching strategy) in the future.
    """

    def __init__(self, duckdb_executor):
        self._classifier = QueryClassifier()
        self._duckdb_executor = duckdb_executor

    def route(self, sql: str) -> Literal["duckdb", "duckdb_light"]:
        """Classify the query as heavy or light.

        Args:
            sql: The SQL query to classify.

        Returns:
            ``"duckdb"`` for heavy queries, ``"duckdb_light"`` for light queries.
        """
        if self._classifier.is_heavy_query(sql):
            return "duckdb"
        return "duckdb_light"

    def execute(self, sql: str, table_refs: List[str]) -> Dict:
        """Execute the SQL query via DuckDB.

        Args:
            sql: The SQL query to execute.
            table_refs: Table names referenced in the SQL.

        Returns:
            A dict with keys:
            - ``"engine"``: ``"duckdb"`` or ``"duckdb_light"``
            - ``"data"``: ExecutionResult (may have csv_url when row_count > 50)
            - ``"reasons"``: list of classification reasons (empty for light queries)
        """
        reasons = self._classifier.get_reason(sql)
        engine: Literal["duckdb", "duckdb_light"] = "duckdb" if reasons else "duckdb_light"

        logger.info("[QueryRouter] engine=%s reasons=%s", engine, reasons)

        data: ExecutionResult = self._duckdb_executor.execute(sql, table_refs)
        return {"engine": engine, "data": data, "reasons": reasons}
