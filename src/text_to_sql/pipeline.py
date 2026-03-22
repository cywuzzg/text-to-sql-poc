"""Pipeline: orchestrates TableRouter → SQLGenerator → MCPExecutor."""
import logging

from text_to_sql.database.schema_registry import get_schema_detail_for_generation
from text_to_sql.executor.mcp_executor import DirectSQLiteExecutor, MCPExecutor
from text_to_sql.generator.sql_generator import SQLGenerator
from text_to_sql.models.response import PipelineResult
from text_to_sql.router.table_router import TableRouter

logger = logging.getLogger(__name__)


class TextToSQLPipeline:
    def __init__(
        self,
        router: TableRouter,
        generator: SQLGenerator,
        executor,  # MCPExecutor or DirectSQLiteExecutor
    ):
        self._router = router
        self._generator = generator
        self._executor = executor

    def run(self, natural_language: str) -> PipelineResult:
        logger.info("Pipeline start: %r", natural_language)

        # Step 1: route
        route_result = self._router.route(natural_language)
        logger.info("Routed to tables: %s (confidence=%.2f)", route_result.tables, route_result.confidence)

        # Step 2: fetch schema for relevant tables
        schema_context = get_schema_detail_for_generation(route_result.tables)

        # Step 3: generate SQL
        generate_result = self._generator.generate(natural_language, schema_context)
        logger.info("Generated SQL: %s", generate_result.sql)

        # Step 4: execute
        execution_result = self._executor.execute(generate_result.sql)
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
        )


def build_pipeline(db_path: str, use_mcp: bool = True) -> TextToSQLPipeline:
    """Factory: construct a ready-to-use pipeline from config."""
    import anthropic

    from text_to_sql.config import ANTHROPIC_API_KEY, CLAUDE_MODEL

    client = anthropic.Anthropic(api_key=ANTHROPIC_API_KEY)
    router = TableRouter(claude_client=client, model=CLAUDE_MODEL)
    generator = SQLGenerator(claude_client=client, model=CLAUDE_MODEL)
    executor = MCPExecutor(db_path=db_path) if use_mcp else DirectSQLiteExecutor(db_path=db_path)

    return TextToSQLPipeline(router=router, generator=generator, executor=executor)
