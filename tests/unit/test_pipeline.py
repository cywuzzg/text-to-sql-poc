"""Unit tests for TextToSQLPipeline (all components are mocked)."""
from unittest.mock import MagicMock

import pytest

from text_to_sql.models.request import GenerateResult, RouteResult
from text_to_sql.models.response import ExecutionResult, PipelineResult
from text_to_sql.pipeline import TextToSQLPipeline
from text_to_sql.router.table_router import RouteParseError


def _make_execution_result(**kwargs) -> ExecutionResult:
    defaults = dict(success=True, columns=["id"], rows=[[1]], row_count=1)
    defaults.update(kwargs)
    return ExecutionResult(**defaults)


def _make_pipeline(route_result=None, generate_result=None, query_router_result=None):
    router = MagicMock()
    router.route.return_value = route_result or RouteResult(
        tables=["products"], confidence=0.9, reasoning="test"
    )

    generator = MagicMock()
    generator.generate.return_value = generate_result or GenerateResult(
        sql="SELECT * FROM products", explanation="all products"
    )

    query_router = MagicMock()
    query_router.execute.return_value = query_router_result or {
        "engine": "duckdb_light",
        "data": _make_execution_result(columns=["product_id", "name"], rows=[[1, "Widget"]], row_count=1),
        "reasons": [],
    }

    return TextToSQLPipeline(router=router, generator=generator, query_router=query_router)


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
        assert isinstance(call_args.args[1], str)
        assert len(call_args.args[1]) > 0

    def test_pipeline_calls_query_router_with_sql_and_tables(self):
        pipeline = _make_pipeline()
        pipeline.run("test")
        pipeline._query_router.execute.assert_called_once_with(
            "SELECT * FROM products", ["products"]
        )

    def test_router_error_propagates(self):
        pipeline = _make_pipeline()
        pipeline._router.route.side_effect = RouteParseError("bad json")
        with pytest.raises(RouteParseError):
            pipeline.run("ambiguous query")

    def test_executor_failure_included_in_result(self):
        failed_exec = _make_execution_result(
            success=False, columns=[], rows=[], row_count=0, error="syntax error"
        )
        pipeline = _make_pipeline(
            query_router_result={"engine": "duckdb_light", "data": failed_exec, "reasons": []}
        )
        result = pipeline.run("bad sql scenario")
        assert result.execution.success is False
        assert result.execution.error == "syntax error"

    def test_multi_table_route_passes_all_schemas(self):
        route = RouteResult(tables=["orders", "order_items"], confidence=0.9, reasoning="join")
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

    def test_pipeline_result_default_engine_is_duckdb_light(self):
        pipeline = _make_pipeline()
        result = pipeline.run("test")
        assert result.engine == "duckdb_light"

    def test_pipeline_result_default_routing_reasons_is_empty(self):
        pipeline = _make_pipeline()
        result = pipeline.run("test")
        assert result.routing_reasons == []

    def test_heavy_query_engine_is_duckdb(self):
        heavy_result = {
            "engine": "duckdb",
            "data": _make_execution_result(columns=["user_id", "cnt"], rows=[], row_count=0),
            "reasons": ["GROUP BY", "AGGREGATE FUNCTION"],
        }
        pipeline = _make_pipeline(query_router_result=heavy_result)
        result = pipeline.run("每個用戶的訂單數量")
        assert result.engine == "duckdb"
        assert "GROUP BY" in result.routing_reasons

    def test_csv_url_propagated_from_large_result(self):
        large_result = {
            "engine": "duckdb",
            "data": _make_execution_result(rows=[], row_count=200, csv_url="results/large.csv"),
            "reasons": ["FULL_TABLE_SCAN"],
        }
        pipeline = _make_pipeline(query_router_result=large_result)
        result = pipeline.run("SELECT * FROM orders")
        assert result.execution.csv_url == "results/large.csv"
        assert result.execution.row_count == 200
