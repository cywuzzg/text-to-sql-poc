"""Unit tests for TextToSQLPipeline (all components are mocked)."""
from unittest.mock import MagicMock

import pandas as pd
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

    def test_pipeline_result_default_engine_is_main_db(self):
        pipeline = _make_pipeline()
        result = pipeline.run("test")
        assert result.engine == "main_db"

    def test_pipeline_result_default_routing_reasons_is_empty(self):
        pipeline = _make_pipeline()
        result = pipeline.run("test")
        assert result.routing_reasons == []


def _make_pipeline_with_query_router(
    route_result=None,
    generate_result=None,
    query_router_result=None,
):
    router = MagicMock()
    router.route.return_value = route_result or RouteResult(
        tables=["orders"], confidence=0.9, reasoning="test"
    )

    generator = MagicMock()
    generator.generate.return_value = generate_result or GenerateResult(
        sql="SELECT COUNT(*) FROM orders GROUP BY user_id",
        explanation="count per user",
    )

    query_router = MagicMock()
    query_router.execute.return_value = query_router_result or {
        "engine": "duckdb",
        "data": pd.DataFrame({"user_id": [1, 2], "cnt": [3, 5]}),
        "reasons": ["GROUP BY", "AGGREGATE FUNCTION"],
    }

    return TextToSQLPipeline(
        router=router,
        generator=generator,
        executor=MagicMock(),
        query_router=query_router,
    )


class TestTextToSQLPipelineWithQueryRouter:
    def test_uses_query_router_instead_of_executor(self):
        pipeline = _make_pipeline_with_query_router()
        pipeline.run("每個用戶的訂單數量")
        pipeline._query_router.execute.assert_called_once()
        pipeline._executor.execute.assert_not_called()

    def test_query_router_called_with_sql_and_tables(self):
        route = RouteResult(tables=["orders"], confidence=0.9, reasoning="test")
        generate = GenerateResult(
            sql="SELECT COUNT(*) FROM orders GROUP BY user_id",
            explanation="count per user",
        )
        pipeline = _make_pipeline_with_query_router(
            route_result=route, generate_result=generate
        )
        pipeline.run("每個用戶的訂單數量")

        call_args = pipeline._query_router.execute.call_args
        assert call_args.args[0] == "SELECT COUNT(*) FROM orders GROUP BY user_id"
        assert call_args.kwargs.get("table_refs") == ["orders"] or call_args.args[1] == ["orders"]

    def test_result_engine_reflects_query_router_decision(self):
        pipeline = _make_pipeline_with_query_router()
        result = pipeline.run("每個用戶的訂單數量")
        assert result.engine == "duckdb"

    def test_result_routing_reasons_populated_for_heavy_query(self):
        pipeline = _make_pipeline_with_query_router()
        result = pipeline.run("每個用戶的訂單數量")
        assert "GROUP BY" in result.routing_reasons

    def test_result_engine_main_db_for_light_query(self):
        pipeline = _make_pipeline_with_query_router(
            generate_result=GenerateResult(
                sql="SELECT * FROM users WHERE id = 1 LIMIT 5",
                explanation="find user",
            ),
            query_router_result={
                "engine": "main_db",
                "data": MagicMock(
                    spec=ExecutionResult,
                    success=True,
                    columns=["id"],
                    rows=[[1]],
                    row_count=1,
                ),
                "reasons": [],
            },
        )
        result = pipeline.run("找特定用戶")
        assert result.engine == "main_db"
        assert result.routing_reasons == []

    def test_duckdb_dataframe_converted_to_execution_result(self):
        pipeline = _make_pipeline_with_query_router()
        result = pipeline.run("每個用戶的訂單數量")
        assert isinstance(result.execution, ExecutionResult)
        assert result.execution.success is True
        assert "user_id" in result.execution.columns

    def test_main_db_execution_result_passed_through(self):
        exec_result = MagicMock(spec=ExecutionResult)
        exec_result.success = True
        exec_result.columns = ["id", "name"]
        exec_result.rows = [[1, "Alice"]]
        exec_result.row_count = 1
        exec_result.error = None

        pipeline = _make_pipeline_with_query_router(
            query_router_result={
                "engine": "main_db",
                "data": exec_result,
                "reasons": [],
            }
        )
        result = pipeline.run("test")
        assert result.execution == exec_result
