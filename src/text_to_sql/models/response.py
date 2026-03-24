from typing import Any, List, Literal, Optional

from pydantic import BaseModel

from text_to_sql.models.request import GenerateResult, RouteResult


class ExecutionResult(BaseModel):
    success: bool
    columns: List[str]
    rows: List[List[Any]]
    row_count: int
    error: Optional[str] = None


class PipelineResult(BaseModel):
    query: str
    route: RouteResult
    generated: GenerateResult
    execution: ExecutionResult
    engine: Literal["main_db", "duckdb"] = "main_db"
    routing_reasons: List[str] = []
