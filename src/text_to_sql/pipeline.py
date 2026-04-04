"""Pipeline: orchestrates TableRouter → SQLGenerator → QueryRouter."""
import logging
import os

from text_to_sql.database.schema_registry import get_schema_detail_for_generation
from text_to_sql.generator.sql_generator import SQLGenerator
from text_to_sql.models.response import ExecutionResult, PipelineResult
from text_to_sql.router.table_router import TableRouter

logger = logging.getLogger(__name__)


class TextToSQLPipeline:
    def __init__(
        self,
        router: TableRouter,
        generator: SQLGenerator,
        query_router,  # QueryRouter — routes all queries through DuckDB
    ):
        self._router = router
        self._generator = generator
        self._query_router = query_router

    def run(self, natural_language: str) -> PipelineResult:
        logger.info("Pipeline start: %r", natural_language)

        # Step 1: route to relevant tables
        route_result = self._router.route(natural_language)
        logger.info(
            "Routed to tables: %s (confidence=%.2f)",
            route_result.tables,
            route_result.confidence,
        )

        # Step 2: fetch schema for relevant tables
        schema_context = get_schema_detail_for_generation(route_result.tables)

        # Step 3: generate SQL
        generate_result = self._generator.generate(natural_language, schema_context)
        logger.info("Generated SQL: %s", generate_result.sql)

        # Step 4: execute via QueryRouter (always DuckDB)
        routing = self._query_router.execute(generate_result.sql, route_result.tables)
        engine: str = routing["engine"]
        reasons = routing["reasons"]
        execution_result: ExecutionResult = routing["data"]

        logger.info(
            "Execution via %s (%s): row_count=%d csv_url=%s reasons=%s",
            engine,
            "OK" if execution_result.success else "FAILED",
            execution_result.row_count,
            execution_result.csv_url,
            reasons,
        )

        return PipelineResult(
            query=natural_language,
            route=route_result,
            generated=generate_result,
            execution=execution_result,
            engine=engine,
            routing_reasons=reasons,
        )


def build_pipeline() -> TextToSQLPipeline:
    """Factory: construct a ready-to-use pipeline.

    Reads configuration from environment variables:
    - ANTHROPIC_API_KEY (required)
    - CLAUDE_MODEL
    - MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY, MINIO_BUCKET
    """
    import anthropic
    from minio import Minio

    from text_to_sql.config import ANTHROPIC_API_KEY, CLAUDE_MODEL, MINIO_BUCKET, MINIO_ENDPOINT, MINIO_ACCESS_KEY, MINIO_SECRET_KEY
    from text_to_sql.database.schema_registry import get_all_table_names
    from text_to_sql.routing.duckdb_executor import DuckDBExecutor
    from text_to_sql.routing.query_router import QueryRouter

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    table_router = TableRouter(claude_client=client, model=CLAUDE_MODEL)
    generator = SQLGenerator(claude_client=client, model=CLAUDE_MODEL)

    minio_client = Minio(
        endpoint=MINIO_ENDPOINT,
        access_key=MINIO_ACCESS_KEY,
        secret_key=MINIO_SECRET_KEY,
        secure=False,
    )
    duckdb_executor = DuckDBExecutor(
        minio_client=minio_client,
        bucket=MINIO_BUCKET,
        table_names=get_all_table_names(),
    )
    query_router = QueryRouter(duckdb_executor=duckdb_executor)

    return TextToSQLPipeline(
        router=table_router,
        generator=generator,
        query_router=query_router,
    )
