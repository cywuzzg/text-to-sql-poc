"""
Integration tests — require a real ANTHROPIC_API_KEY and the ecommerce.db.

Run with:
    ANTHROPIC_API_KEY=xxx pytest tests/integration/ -m integration -v
"""
import os

import pytest

from text_to_sql.pipeline import build_pipeline

SKIP_REASON = "ANTHROPIC_API_KEY not set"


def _has_api_key() -> bool:
    return bool(os.environ.get("ANTHROPIC_API_KEY"))


@pytest.fixture(scope="module")
def pipeline():
    if not _has_api_key():
        pytest.skip(SKIP_REASON)
    return build_pipeline(db_path="database/ecommerce.db", use_mcp=False)


@pytest.mark.integration
class TestFullPipelineIntegration:
    def test_single_table_product_stock_query(self, pipeline):
        result = pipeline.run("庫存不足 50 件的商品有哪些？")
        assert result.execution.success is True
        assert "products" in result.route.tables
        assert result.execution.row_count >= 0

    def test_multi_table_sales_analysis(self, pipeline):
        result = pipeline.run("最近銷售額最高的前 3 件商品是什麼？")
        assert result.execution.success is True
        assert result.execution.row_count <= 3

    def test_user_region_analysis(self, pipeline):
        result = pipeline.run("哪個地區的用戶消費總金額最高？")
        assert result.execution.success is True
        assert result.execution.row_count >= 1

    def test_generated_sql_is_select(self, pipeline):
        result = pipeline.run("所有用戶的基本資料")
        assert result.generated.sql.strip().upper().startswith("SELECT")

    def test_explanation_is_non_empty(self, pipeline):
        result = pipeline.run("電子產品類別的商品有哪些？")
        assert result.generated.explanation.strip() != ""
