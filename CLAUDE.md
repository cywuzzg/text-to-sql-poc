# Text-to-SQL POC — Claude 開發指引

## Project Overview

這是一個概念驗證（POC）專案，示範如何透過 Claude API 將自然語言查詢轉換為 SQLite SQL，並自動執行回傳結果。採用電商場景（users / products / orders / order_items）作為示範資料庫。

```
自然語言 → TableRouter → SQLGenerator → MCPExecutor → 查詢結果
```

---

## Quick Start

```bash
# 1. 確認 Python ≥ 3.11（系統 python3 是 3.9，需用 brew 版本）
which python3.11   # 應為 /opt/homebrew/bin/python3.11

# 2. 建立虛擬環境（第一次）
python3.11 -m venv .venv
source .venv/bin/activate
pip install -e ".[dev]"

# 3. 之後啟動
source .venv/bin/activate

# 4. 設定環境變數
cp .env.example .env
# 填入 ANTHROPIC_API_KEY

# 5. 初始化資料庫
python scripts/init_db.py

# 6. 執行互動 demo
python scripts/demo.py
```

---

## Architecture

三元件串聯，每個元件介面獨立，可替換：

```
用戶輸入（自然語言）
        │
        ▼
┌───────────────────────────────────────────────────────┐
│                   TextToSQLPipeline                   │
│  pipeline.py                                          │
│                                                       │
│  1. TableRouter.route(query)                          │
│     → RouteResult { tables, confidence, reasoning }   │
│                                                       │
│  2. SchemaRegistry.get_schema_detail_for_generation() │
│     → schema_context (DDL + 欄位說明字串)             │
│                                                       │
│  3. SQLGenerator.generate(query, schema_context)      │
│     → GenerateResult { sql, explanation }             │
│                                                       │
│  4. Executor.execute(sql)                             │
│     → ExecutionResult { success, columns, rows, ... } │
│                                                       │
│  5. 組裝 PipelineResult 回傳                           │
└───────────────────────────────────────────────────────┘
```

### 關鍵檔案對照表

| 元件 / 用途 | 路徑 |
|---|---|
| Pipeline 編排器 | `src/text_to_sql/pipeline.py` |
| Pipeline 工廠函數 | `pipeline.build_pipeline()` |
| Table Router | `src/text_to_sql/router/table_router.py` |
| Router prompt 模板 | `src/text_to_sql/router/prompts.py` |
| SQL Generator | `src/text_to_sql/generator/sql_generator.py` |
| Generator prompt 模板 | `src/text_to_sql/generator/prompts.py` |
| MCP Executor（正式用） | `src/text_to_sql/executor/mcp_executor.MCPExecutor` |
| SQLite Executor（測試/fallback） | `src/text_to_sql/executor/mcp_executor.DirectSQLiteExecutor` |
| Schema Registry（唯一真相） | `src/text_to_sql/database/schema_registry.py` |
| DDL 語句 | `src/text_to_sql/database/schema.py` |
| Seed 資料 | `src/text_to_sql/database/seed.py` |
| Pydantic models | `src/text_to_sql/models/request.py` + `response.py` |
| 環境變數設定 | `src/text_to_sql/config.py` |
| 測試 fixtures | `tests/conftest.py` |

---

## Development Rules

CLAUDE.md 的 **全域規則**（`~/.claude/CLAUDE.md`）：
- 每完成一個步驟請 commit
- 每個 function 都需要先寫單元測試
- 每個功能都要先產生測試案例及對應文件再實作
- 所有的更改都需要先完成測試與文件的修改再實作功能

**本專案補充規則**：
- 新增任何表 → 同步更新 `schema.py`、`schema_registry.py`、`seed.py`
- 改 prompt → 同步更新 `docs/examples.md` 中的預期輸出
- 整合測試用 `pytest -m integration`（需 API key），不要在沒有 key 的 CI 上跑

---

## Testing

```bash
# 啟動 venv
source .venv/bin/activate

# 單元測試（無需 API key）
pytest tests/unit/ -v

# 含覆蓋率
pytest tests/unit/ --cov=src/text_to_sql --cov-report=html

# 整合測試（需要 ANTHROPIC_API_KEY）
ANTHROPIC_API_KEY=xxx pytest tests/integration/ -m integration -v
```

### Mock 策略
- **Claude API**：用 `unittest.mock.MagicMock` 替換 `anthropic.Anthropic`，設定 `messages.create.return_value`
- **MCP Server**：在 fixture 中注入 `DirectSQLiteExecutor(conn=in_memory_db)`，繞過 subprocess

Mock 範例（見 `tests/unit/test_table_router.py`）：
```python
message = MagicMock()
message.content = [MagicMock(text=json.dumps({"tables": ["products"], "confidence": 0.9, "reasoning": "..."})) ]
mock_client.messages.create.return_value = message
```

---

## Key Patterns

### 新增一張資料表

1. **`src/text_to_sql/database/schema.py`** — 加入 `CREATE TABLE` 語句到 `DDL_STATEMENTS`

2. **`src/text_to_sql/database/schema_registry.py`** — 在 `_REGISTRY` dict 新增 `TableSchema`：
   ```python
   "new_table": TableSchema(
       name="new_table",
       description="表的中文說明",
       columns=[ColumnInfo("col", "TYPE", "欄位說明"), ...],
       ddl="CREATE TABLE new_table (...)",
       example_queries=["查詢範例1", "查詢範例2"],
   )
   ```

3. **`src/text_to_sql/database/seed.py`** — 新增種子資料和插入邏輯

4. **`tests/unit/test_schema_registry.py`** — 更新 `test_all_four_tables_exist` 等測試

5. **`docs/database_schema.md`** — 更新文件

### 修改 Prompt

- **Router prompt**：`src/text_to_sql/router/prompts.py` 的 `ROUTER_SYSTEM_TEMPLATE`
  - `{schema_summary}` 由 `get_schema_summary_for_routing()` 自動填入
- **Generator prompt**：`src/text_to_sql/generator/prompts.py` 的 `GENERATOR_SYSTEM_TEMPLATE`
  - `{schema_context}` 由 `get_schema_detail_for_generation(tables)` 填入

### 切換 Executor

```python
# 使用 MCP（需要 uvx）
pipeline = build_pipeline(db_path="database/ecommerce.db", use_mcp=True)

# 使用 DirectSQLite（fallback，無需 uvx）
pipeline = build_pipeline(db_path="database/ecommerce.db", use_mcp=False)
```

### 切換 Claude 模型

`.env` 中設定：
```
CLAUDE_MODEL=claude-sonnet-4-6
```

---

## Known Constraints

| 限制 | 說明 |
|---|---|
| Python ≥ 3.11 | `mcp` SDK 要求；系統 python3 是 3.9，需用 `.venv` |
| 只允許 SELECT | `SQLGenerator._validate_sql()` 拒絕所有非 SELECT 語句 |
| MCPExecutor 需 `uvx` | `pip install uv` 後即可用；否則改用 `use_mcp=False` |
| JSON 輸出 | Router 和 Generator 都依賴 Claude 輸出純 JSON；若出現解析錯誤，先檢查 prompt |
| 低信心路由 | confidence < 0.5 時只 log 警告，不中斷流程 |

---

## Useful Commands

```bash
# 重新初始化資料庫（清空後重建）
python scripts/init_db.py

# 查看資料庫內容
python -c "
import sqlite3
conn = sqlite3.connect('database/ecommerce.db')
for t in ['users','products','orders','order_items']:
    n = conn.execute(f'SELECT COUNT(*) FROM {t}').fetchone()[0]
    print(f'{t}: {n} rows')
"

# 格式化 git log
git log --oneline
```
