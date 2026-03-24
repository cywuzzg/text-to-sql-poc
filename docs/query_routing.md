# Query Routing — 設計文件

## 概覽

在現有 text-to-SQL agent 基礎上加入查詢路由層。當 AI 生成的 SQL 被靜態分析判定為「重型查詢」時，自動將資料匯出至 MinIO，再用 DuckDB 執行，避免直接對主資料庫造成壓力。

```
SQL (from SQLGenerator)
        │
        ▼
┌───────────────────┐
│   QueryRouter     │
│                   │
│  QueryClassifier  │──→ is_heavy?
│      ↓            │
│  "main_db"        │──→ DirectSQLiteExecutor / MCPExecutor
│  "duckdb"         │──→ DuckDBExecutor (export → MinIO → DuckDB)
└───────────────────┘
        │
        ▼
   { engine, data, reasons }
```

---

## 元件說明

### 1. QueryClassifier (`src/text_to_sql/routing/query_classifier.py`)

靜態規則分析器，不需要執行 SQL，純粹對 SQL 文字做關鍵字分析。

#### 重型查詢判斷規則

| 規則 | 範例 | 說明 |
|------|------|------|
| `GROUP BY` | `SELECT user_id, COUNT(*) FROM orders GROUP BY user_id` | 分組聚合 |
| `HAVING` | `... HAVING COUNT(*) > 5` | 分組過濾 |
| `WINDOW FUNCTION` | `ROW_NUMBER() OVER (...)` | 視窗函數 |
| `CROSS JOIN` | `FROM a CROSS JOIN b` | 笛卡爾積 |
| 聚合函數 | `COUNT(`, `SUM(`, `AVG(`, `MAX(`, `MIN(` | 全表聚合 |
| 無 LIMIT 且無 WHERE | `SELECT * FROM orders` | 全表掃描風險 |
| 深層子查詢 | 超過 2 層 nested SELECT | 複雜子查詢 |

#### 方法

```python
classifier = QueryClassifier()

# 判斷是否為重型查詢
classifier.is_heavy_query("SELECT COUNT(*) FROM orders")  # True

# 取得觸發原因（方便 log 與 debug）
classifier.get_reason("SELECT COUNT(*) FROM orders")  # ["AGGREGATE FUNCTION"]
```

---

### 2. DuckDBExecutor (`src/text_to_sql/routing/duckdb_executor.py`)

將主資料庫資料匯出至 MinIO，再用 DuckDB 執行查詢。

#### MinIO 環境變數設定

| 環境變數 | 說明 | 範例 |
|---------|------|------|
| `MINIO_ENDPOINT` | MinIO 伺服器位址 | `localhost:9000` |
| `MINIO_ACCESS_KEY` | Access Key | `minioadmin` |
| `MINIO_SECRET_KEY` | Secret Key | `minioadmin` |
| `MINIO_BUCKET` | 儲存桶名稱 | `text-to-sql` |

#### 方法

```python
executor = DuckDBExecutor(db_path="database/ecommerce.db", minio_client=client, bucket="text-to-sql")

# 匯出單張資料表至 MinIO，回傳 S3 路徑
path = executor.export_table_to_minio("orders")
# -> "s3://text-to-sql/orders.parquet"

# 執行 SQL（自動掛載所需資料表）
df = executor.execute("SELECT COUNT(*) FROM orders", table_refs=["orders"])
```

#### 執行流程

1. 對每個 `table_ref` 呼叫 `export_table_to_minio()`
2. 建立 DuckDB 連線，安裝 `httpfs` extension
3. 設定 S3 認證（對應 MinIO）
4. 為每個資料表建立 VIEW，指向 MinIO 上的 parquet 檔
5. 在 DuckDB 內執行原始 SQL，回傳 `pd.DataFrame`

---

### 3. QueryRouter (`src/text_to_sql/routing/query_router.py`)

整合 `QueryClassifier` 和兩種執行引擎的路由層。

#### 方法

```python
router = QueryRouter(
    main_db_executor=DirectSQLiteExecutor(db_path="..."),
    duckdb_executor=DuckDBExecutor(...),
)

# 判斷使用哪個引擎
engine = router.route("SELECT COUNT(*) FROM orders")  # "duckdb"

# 自動路由並執行
result = router.execute("SELECT COUNT(*) FROM orders", table_refs=["orders"])
# -> {
#      "engine": "duckdb",
#      "data": pd.DataFrame(...),
#      "reasons": ["AGGREGATE FUNCTION"]
#    }
```

#### 回傳格式

```python
{
    "engine": "main_db" | "duckdb",
    "data": pd.DataFrame,
    "reasons": list[str]   # 觸發重型查詢的原因（輕型查詢為空 list）
}
```

---

## Log 格式

所有路由決策都會寫入 log：

```
[QueryRouter] engine=duckdb reasons=[GROUP BY, no WHERE]
[QueryRouter] engine=main_db reasons=[]
```

---

## Pipeline 整合

`TextToSQLPipeline` 在 Step 4 (Execute) 時改用 `QueryRouter.execute()`：

```python
# 原本
execution_result = self._executor.execute(generated_sql)

# 整合後
route_result = self._query_router.execute(generated_sql, table_refs=route_result.tables)
# 回應給使用者時附帶說明使用了哪個引擎及原因
```

---

## 測試策略

- **QueryClassifier**：純單元測試，無需外部依賴
- **DuckDBExecutor**：mock MinIO client 和 sqlite3 連線，mock duckdb
- **QueryRouter**：mock QueryClassifier + 兩個 executor

參見 `tests/unit/test_query_classifier.py`、`test_duckdb_executor.py`、`test_query_router.py`
