# 系統架構

## 概覽

Text-to-SQL POC 透過三個串聯元件，將自然語言查詢轉換為 SQL 並執行：

```
用戶輸入（自然語言）
        │
        ▼
  ┌─────────────┐
  │ TableRouter │  ← Claude API（判斷查哪些表）
  └──────┬──────┘
         │ RouteResult { tables, confidence, reasoning }
         ▼
  ┌────────────────┐
  │ SchemaRegistry │  ← 唯一真相來源（無 LLM 依賴）
  └───────┬────────┘
          │ schema_context（DDL + 欄位說明字串）
          ▼
  ┌──────────────┐
  │ SQLGenerator │  ← Claude API（生成 SELECT SQL）
  └──────┬───────┘
         │ GenerateResult { sql, explanation }
         ▼
  ┌──────────────┐
  │   Executor   │  ← MCPExecutor 或 DirectSQLiteExecutor
  └──────┬───────┘
         │ ExecutionResult { success, columns, rows, row_count }
         ▼
   PipelineResult（含完整查詢鏈路 + 執行結果）
```

---

## 元件職責

### TextToSQLPipeline
`src/text_to_sql/pipeline.py`

**唯一職責**：串接三個元件，傳遞資料。不包含任何業務邏輯。

```python
class TextToSQLPipeline:
    def run(self, natural_language: str) -> PipelineResult
```

工廠函數：
```python
build_pipeline(db_path: str, use_mcp: bool = True) -> TextToSQLPipeline
```

---

### TableRouter
`src/text_to_sql/router/table_router.py`

**職責**：接收自然語言，判斷需要查哪些表。

```python
class TableRouter:
    def route(self, query: str) -> RouteResult
```

**輸入**：自然語言字串（非空）
**輸出**：
```python
RouteResult(
    tables=["products", "orders"],  # 相關表名，從 schema registry 的表中選
    confidence=0.92,                # 0.0～1.0，低於 0.5 時 log 警告
    reasoning="需要 JOIN products 和 orders 才能計算銷售額"
)
```

**錯誤**：
- `ValueError`：空查詢
- `RouteParseError`：Claude 回傳非法 JSON 或缺少 `tables` 欄位

**Claude 呼叫方式**：
- System prompt = `ROUTER_SYSTEM_TEMPLATE`（含所有表的摘要）
- User message = 自然語言查詢
- 要求 Claude 回傳純 JSON：`{"tables": [...], "confidence": ..., "reasoning": "..."}`

---

### SQLGenerator
`src/text_to_sql/generator/sql_generator.py`

**職責**：接收自然語言 + schema 上下文，生成 SQLite SELECT 語句。

```python
class SQLGenerator:
    def generate(self, query: str, schema_context: str) -> GenerateResult
```

**輸入**：
- `query`：原始自然語言查詢
- `schema_context`：由 SchemaRegistry 提供的完整 DDL + 欄位說明

**輸出**：
```python
GenerateResult(
    sql="SELECT p.name, SUM(oi.quantity) FROM products p ...",
    explanation="統計各商品的銷售總數量"
)
```

**安全驗證**（`_validate_sql()`）：
- SQL 必須以 `SELECT` 開頭
- 禁止關鍵字：`INSERT, UPDATE, DELETE, DROP, CREATE, ALTER, TRUNCATE, REPLACE`
- 違規拋出 `UnsafeSQLError`

**Claude 呼叫方式**：
- System prompt = `GENERATOR_SYSTEM_TEMPLATE`（含完整 schema context）
- 要求 Claude 回傳純 JSON：`{"sql": "...", "explanation": "..."}`

---

### SchemaRegistry
`src/text_to_sql/database/schema_registry.py`

**職責**：作為所有表 schema 資訊的唯一真相來源（single source of truth）。

**不呼叫任何外部 API**，純粹是 Python dict。

```python
# 給 Router 用（簡短摘要，不含完整 DDL）
get_schema_summary_for_routing() -> str

# 給 Generator 用（完整 DDL + 欄位說明）
get_schema_detail_for_generation(tables: List[str]) -> str

get_all_table_names() -> List[str]
get_table_schema(table_name: str) -> TableSchema  # 找不到拋 KeyError
```

**設計原則**：Router 只需要知道「有哪些表」以決定路由；Generator 才需要完整 DDL。因此透過這兩個不同的函數分別提供精簡和完整的資訊，避免 Token 浪費。

---

### Executor
`src/text_to_sql/executor/mcp_executor.py`

兩種實作，介面相同：

```python
class DirectSQLiteExecutor:
    def __init__(self, db_path: str = "", conn: sqlite3.Connection = None)
    def execute(self, sql: str) -> ExecutionResult

class MCPExecutor:
    def __init__(self, db_path: str)
    def execute(self, sql: str) -> ExecutionResult
```

**DirectSQLiteExecutor**：
- 直接使用 Python `sqlite3` 模組
- 用於單元測試（注入 in-memory DB）和無 uvx 環境的 fallback
- 異常捕獲後回傳 `ExecutionResult(success=False, error=...)`

**MCPExecutor**：
- 透過 `mcp` SDK stdio transport 啟動 `uvx mcp-server-sqlite` 子程序
- 非同步實作包裝為同步（`asyncio.run()`）
- 呼叫 MCP tool：`read_query`，傳入 SQL
- 需要系統安裝 `uv`（`pip install uv`）

---

## 資料模型

```python
# 輸入
QueryRequest(natural_language: str)       # 驗證非空

# 中間狀態
RouteResult(tables, confidence, reasoning)
GenerateResult(sql, explanation)

# 輸出
ExecutionResult(success, columns, rows, row_count, error?)
PipelineResult(query, route, generated, execution)
```

所有 model 定義在 `src/text_to_sql/models/`，使用 Pydantic v2。

---

## 設定

`src/text_to_sql/config.py` 從 `.env` 讀取：

| 變數 | 預設值 | 說明 |
|---|---|---|
| `ANTHROPIC_API_KEY` | （必填） | Claude API key |
| `CLAUDE_MODEL` | `claude-haiku-4-5-20251001` | 可改為 claude-sonnet-4-6 等 |
| `DB_PATH` | `database/ecommerce.db` | SQLite 資料庫路徑 |

---

## 錯誤處理策略

| 錯誤 | 發生位置 | 處理方式 |
|---|---|---|
| `ValueError("Query must not be empty")` | TableRouter | Pipeline 向上傳播 |
| `RouteParseError` | TableRouter | Pipeline 向上傳播 |
| `UnsafeSQLError` | SQLGenerator | Pipeline 向上傳播 |
| `ValueError` | SQLGenerator（JSON 解析失敗） | Pipeline 向上傳播 |
| SQL 執行錯誤 | DirectSQLiteExecutor / MCPExecutor | 捕獲，回傳 `ExecutionResult(success=False)` |

Pipeline 本身不捕獲異常（讓呼叫者決定如何處理），只有 Executor 層做防禦性捕獲。

---

## 測試架構

```
tests/
├── conftest.py          # 共用 fixtures
├── unit/                # 不需要 API key，快速
│   ├── test_models.py
│   ├── test_schema_registry.py
│   ├── test_mcp_executor.py
│   ├── test_table_router.py    # mock Claude
│   ├── test_sql_generator.py   # mock Claude
│   └── test_pipeline.py        # mock 所有元件
└── integration/         # 需要 ANTHROPIC_API_KEY
    └── test_full_pipeline.py
```

單元測試執行速度：56 個測試 < 1 秒（全部 mock，無 I/O）。
