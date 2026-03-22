from typing import List

from pydantic import BaseModel, field_validator


class QueryRequest(BaseModel):
    natural_language: str

    @field_validator("natural_language")
    @classmethod
    def must_not_be_empty(cls, v: str) -> str:
        if not v.strip():
            raise ValueError("Query must not be empty")
        return v.strip()


class RouteResult(BaseModel):
    tables: List[str]
    confidence: float
    reasoning: str


class GenerateResult(BaseModel):
    sql: str
    explanation: str
