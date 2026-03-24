"""Pipeline: orchestrates TableRouter → SQLGenerator → QueryRouter/Executor."""
import logging
from typing import Any, List, Optional

import pandas as pd

from text_to_sql.database.schema_registry import get_schema_detail_for_generation
from text_to_sql.executor.mcp_executor import DirectSQLiteExecutor, MCPExecutor
from text_to_sql.generator.sql_generator import SQLGenerator
from text_to_sql.models.response import ExecutionResult, PipelineResult
from text_to_sql.router.table_router import TableRouter

logger = logging.getLogger(__name__)


def _dataframe_to_execution_result(df: pd.DataFrame) -> ExecutionResult:
    """Convert a pandas DataFrame to ExecutionResult for uniform response format."""
    columns: List[str] = list(df.columns)
    rows: List[List[Any]] = df.values.tolist()
    return ExecutionResult(
        success=True,
        columns=columns,
        rows=rows,
        row_count=len(rows),
    )


class TextToSQLPipeline:
    def __init__(
        self,
        router: TableRouter,
        generator: SQLGenerator,
        executor,  # MCPExecutor or DirectSQLiteExecutor (used when query_router is None)
        query_router=None,  # Optional QueryRouter; when provided, replaces direct executor
    ):
        self._router = router
        self._generator = generator
        self._executor = executor
        self._query_router = query_router

    def run(self, natural_language: str) -> PipelineResult:
        logger.info("Pipeline start: %r", natural_language)

        # Step 1: route to relevant tables
        route_result = self._router.route(natural_language)
        logger.info("Routed to tables: %s (confidence=%.2f)", route_result.tables, route_result.confidence)

        # Step 2: fetch schema for relevant tables
        schema_context = get_schema_detail_for_generation(route_result.tables)

        # Step 3: generate SQL
        generate_result = self._generator.generate(natural_language, schema_context)
        logger.info("Generated SQL: %s", generate_result.sql)

        # Step 4: execute via QueryRouter (if available) or direct executor
        if self._query_router is not None:
            routing = self._query_router.execute(
                generate_result.sql, route_result.tables
            )
            engine = routing["engine"]
            reasons = routing["reasons"]
            data = routing["data"]

            if isinstance(data, pd.DataFrame):
                execution_result = _dataframe_to_execution_result(data)
            else:
                execution_result = data  # already an ExecutionResult from main_db

            logger.info(
                "Execution via %s (%s): %d rows, reasons=%s",
                engine,
                "OK" if execution_result.success else "FAILED",
                execution_result.row_count,
                reasons,
            )
        else:
            execution_result = self._executor.execute(generate_result.sql)
            engine = "main_db"
            reasons = []
            logger.info(
                "Execution %s: %d rows",
                "OK" if execution_result.success else "FAILED",
                execution_result.row_count,
            )

        return PipelineResult(
            query=natural_language,
            route=route_result,
            generated=generate_result,
            execution=execution_result,
            engine=engine,
            routing_reasons=reasons,
        )


def build_pipeline(
    db_path: str,
    use_mcp: bool = True,
    use_query_router: bool = False,
) -> TextToSQLPipeline:
    """Factory: construct a ready-to-use pipeline from config.

    Args:
        db_path: Path to the SQLite database file.
        use_mcp: Use MCPExecutor (requires uvx). Falls back to DirectSQLiteExecutor.
        use_query_router: Wrap execution in QueryRouter for heavy-query offloading to DuckDB.
                          Requires MINIO_* environment variables to be set.
    """
    import anthropic

    from text_to_sql.config import ANTHROPIC_API_KEY, CLAUDE_MODEL

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    table_router = TableRouter(claude_client=client, model=CLAUDE_MODEL)
    generator = SQLGenerator(claude_client=client, model=CLAUDE_MODEL)
    executor = MCPExecutor(db_path=db_path) if use_mcp else DirectSQLiteExecutor(db_path=db_path)

    query_router = None
    if use_query_router:
        import os

        from minio import Minio

        from text_to_sql.routing.duckdb_executor import DuckDBExecutor
        from text_to_sql.routing.query_router import QueryRouter

        minio_client = Minio(
            endpoint=os.environ.get("MINIO_ENDPOINT", "localhost:9000"),
            access_key=os.environ.get("MINIO_ACCESS_KEY", "minioadmin"),
            secret_key=os.environ.get("MINIO_SECRET_KEY", "minioadmin"),
            secure=False,
        )
        bucket = os.environ.get("MINIO_BUCKET", "text-to-sql")
        duckdb_executor = DuckDBExecutor(
            db_path=db_path,
            minio_client=minio_client,
            bucket=bucket,
        )
        main_db_executor = DirectSQLiteExecutor(db_path=db_path)
        query_router = QueryRouter(
            main_db_executor=main_db_executor,
            duckdb_executor=duckdb_executor,
        )

    return TextToSQLPipeline(
        router=table_router,
        generator=generator,
        executor=executor,
        query_router=query_router,
    )
