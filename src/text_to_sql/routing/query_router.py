"""Query router: classify SQL and dispatch to the appropriate execution engine."""
import logging
from typing import Dict, List, Literal

from text_to_sql.routing.query_classifier import QueryClassifier

logger = logging.getLogger(__name__)


class QueryRouter:
    """Route SQL queries to either the main DB or DuckDB based on static analysis.

    Light queries go to the main DB executor.
    Heavy queries are offloaded to DuckDB via MinIO parquet files.
    """

    def __init__(self, main_db_executor, duckdb_executor):
        self._classifier = QueryClassifier()
        self._main_db_executor = main_db_executor
        self._duckdb_executor = duckdb_executor

    def route(self, sql: str) -> Literal["main_db", "duckdb"]:
        """Determine which engine should execute the given SQL.

        Args:
            sql: The SQL query to classify.

        Returns:
            ``"duckdb"`` if the query is heavy, ``"main_db"`` otherwise.
        """
        if self._classifier.is_heavy_query(sql):
            return "duckdb"
        return "main_db"

    def execute(self, sql: str, table_refs: List[str]) -> Dict:
        """Route and execute the SQL query.

        Args:
            sql: The SQL query to execute.
            table_refs: Table names referenced in the SQL (needed by DuckDB executor).

        Returns:
            A dict with keys:
            - ``"engine"``: ``"main_db"`` or ``"duckdb"``
            - ``"data"``: query result (``ExecutionResult`` for main_db, ``pd.DataFrame`` for duckdb)
            - ``"reasons"``: list of reasons that triggered heavy classification (empty for light)
        """
        reasons = self._classifier.get_reason(sql)
        engine = "duckdb" if reasons else "main_db"

        logger.info("[QueryRouter] engine=%s reasons=%s", engine, reasons)

        if engine == "duckdb":
            data = self._duckdb_executor.execute(sql, table_refs)
        else:
            data = self._main_db_executor.execute(sql)

        return {"engine": engine, "data": data, "reasons": reasons}
