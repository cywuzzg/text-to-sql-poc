"""Unit tests for TextToSQLPipeline (all components are mocked)."""
from unittest.mock import MagicMock

import pytest

from text_to_sql.models.request import GenerateResult, RouteResult
from text_to_sql.models.response import ExecutionResult, PipelineResult
from text_to_sql.pipeline import TextToSQLPipeline
from text_to_sql.router.table_router import RouteParseError


def _make_pipeline(route_result=None, generate_result=None, execution_result=None):
    router = MagicMock()
    router.route.return_value = route_result or RouteResult(
        tables=["products"], confidence=0.9, reasoning="test"
    )

    generator = MagicMock()
    generator.generate.return_value = generate_result or GenerateResult(
        sql="SELECT * FROM products", explanation="all products"
    )

    executor = MagicMock()
    executor.execute.return_value = execution_result or ExecutionResult(
        success=True, columns=["product_id", "name"], rows=[[1, "Widget"]], row_count=1
    )

    return TextToSQLPipeline(router=router, generator=generator, executor=executor)


class TestTextToSQLPipeline:
    def test_successful_full_pipeline(self):
        pipeline = _make_pipeline()
        result = pipeline.run("庫存不足的商品有哪些？")

        assert isinstance(result, PipelineResult)
        assert result.query == "庫存不足的商品有哪些？"
        assert result.route.tables == ["products"]
        assert result.generated.sql == "SELECT * FROM products"
        assert result.execution.success is True

    def test_pipeline_calls_router_with_query(self):
        pipeline = _make_pipeline()
        pipeline.run("test query")
        pipeline._router.route.assert_called_once_with("test query")

    def test_pipeline_calls_generator_with_query_and_schema(self):
        pipeline = _make_pipeline()
        pipeline.run("庫存查詢")
        call_args = pipeline._generator.generate.call_args
        assert call_args.args[0] == "庫存查詢"
        # schema context should be a non-empty string
        assert isinstance(call_args.args[1], str)
        assert len(call_args.args[1]) > 0

    def test_pipeline_calls_executor_with_generated_sql(self):
        pipeline = _make_pipeline()
        pipeline.run("test")
        pipeline._executor.execute.assert_called_once_with("SELECT * FROM products")

    def test_router_error_propagates(self):
        pipeline = _make_pipeline()
        pipeline._router.route.side_effect = RouteParseError("bad json")
        with pytest.raises(RouteParseError):
            pipeline.run("ambiguous query")

    def test_executor_failure_included_in_result(self):
        failed_exec = ExecutionResult(
            success=False, columns=[], rows=[], row_count=0, error="syntax error"
        )
        pipeline = _make_pipeline(execution_result=failed_exec)
        result = pipeline.run("bad sql scenario")
        assert result.execution.success is False
        assert result.execution.error == "syntax error"

    def test_multi_table_route_passes_all_schemas(self):
        route = RouteResult(
            tables=["orders", "order_items"], confidence=0.9, reasoning="join"
        )
        pipeline = _make_pipeline(route_result=route)
        pipeline.run("銷售分析")

        schema_arg = pipeline._generator.generate.call_args.args[1]
        assert "orders" in schema_arg
        assert "order_items" in schema_arg

    def test_pipeline_result_contains_full_chain(self):
        pipeline = _make_pipeline()
        result = pipeline.run("full chain test")
        assert result.route is not None
        assert result.generated is not None
        assert result.execution is not None
