import pytest
from pydantic import ValidationError

from text_to_sql.models.request import GenerateResult, QueryRequest, RouteResult
from text_to_sql.models.response import ExecutionResult, PipelineResult


class TestQueryRequest:
    def test_valid_query(self):
        req = QueryRequest(natural_language="庫存不足的商品有哪些？")
        assert req.natural_language == "庫存不足的商品有哪些？"

    def test_strips_whitespace(self):
        req = QueryRequest(natural_language="  query  ")
        assert req.natural_language == "query"

    def test_empty_string_raises(self):
        with pytest.raises(ValidationError):
            QueryRequest(natural_language="")

    def test_whitespace_only_raises(self):
        with pytest.raises(ValidationError):
            QueryRequest(natural_language="   ")


class TestRouteResult:
    def test_valid(self):
        r = RouteResult(tables=["products"], confidence=0.9, reasoning="test")
        assert r.tables == ["products"]

    def test_multiple_tables(self):
        r = RouteResult(tables=["orders", "order_items"], confidence=0.8, reasoning="join needed")
        assert len(r.tables) == 2


class TestExecutionResult:
    def test_success_result(self):
        r = ExecutionResult(success=True, columns=["id", "name"], rows=[[1, "item"]], row_count=1)
        assert r.success is True
        assert r.error is None

    def test_failure_result(self):
        r = ExecutionResult(success=False, columns=[], rows=[], row_count=0, error="syntax error")
        assert r.success is False
        assert r.error == "syntax error"

    def test_empty_result(self):
        r = ExecutionResult(success=True, columns=["id"], rows=[], row_count=0)
        assert r.row_count == 0


class TestPipelineResult:
    def test_pipeline_result_assembly(self):
        route = RouteResult(tables=["products"], confidence=0.9, reasoning="r")
        gen = GenerateResult(sql="SELECT 1", explanation="simple")
        exec_ = ExecutionResult(success=True, columns=["1"], rows=[[1]], row_count=1)
        result = PipelineResult(query="test", route=route, generated=gen, execution=exec_)
        assert result.query == "test"
        assert result.route.tables == ["products"]
