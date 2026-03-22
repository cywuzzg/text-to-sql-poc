# 查詢範例

以下 6 個範例涵蓋常見查詢模式：單表過濾、JOIN、聚合、GROUP BY、ORDER BY。

所有 SQL 均可在 `database/ecommerce.db` 上直接執行驗證：
```bash
source .venv/bin/activate
python -c "
import sqlite3
conn = sqlite3.connect('database/ecommerce.db')
# 將下方 SQL 貼入此處
"
```

---

## 範例 1：單表過濾（庫存查詢）

**自然語言輸入**
```
庫存不足 50 件的商品有哪些？
```

**預期路由**
```json
{
  "tables": ["products"],
  "confidence": 0.95,
  "reasoning": "問題只涉及商品庫存，只需查 products 表"
}
```

**預期 SQL**
```sql
SELECT name, category, price, stock
FROM products
WHERE stock < 50
ORDER BY stock ASC
```

**輸出說明**
- 回傳欄位：name、category、price、stock
- 按庫存升序排列（最稀缺的優先）
- 預期結果約 10～20 筆（依種子資料而定）

---

## 範例 2：單表聚合（品類統計）

**自然語言輸入**
```
各品類分別有幾件商品？
```

**預期路由**
```json
{
  "tables": ["products"],
  "confidence": 0.92,
  "reasoning": "只需 products 表做 GROUP BY"
}
```

**預期 SQL**
```sql
SELECT category, COUNT(*) AS product_count
FROM products
GROUP BY category
ORDER BY product_count DESC
```

**輸出說明**
- 4 筆結果（electronics、clothing、food、home）
- product_count 為各品類商品數量

---

## 範例 3：兩表 JOIN（用戶訂單）

**自然語言輸入**
```
alice 有幾筆訂單？各是什麼狀態？
```

**預期路由**
```json
{
  "tables": ["users", "orders"],
  "confidence": 0.9,
  "reasoning": "需要用 users 找到 alice 的 user_id，再 JOIN orders 查訂單"
}
```

**預期 SQL**
```sql
SELECT o.order_id, o.status, o.total_amount, o.created_at
FROM orders o
JOIN users u ON o.user_id = u.user_id
WHERE u.username = 'alice'
ORDER BY o.created_at DESC
```

**輸出說明**
- 依時間降序顯示 alice 的所有訂單
- 包含訂單狀態（pending/paid/shipped/delivered/cancelled）

---

## 範例 4：地區消費分析（兩表聚合 JOIN）

**自然語言輸入**
```
哪個地區的用戶消費總金額最高？
```

**預期路由**
```json
{
  "tables": ["users", "orders"],
  "confidence": 0.88,
  "reasoning": "需要 users.region 和 orders.total_amount 做 GROUP BY 聚合"
}
```

**預期 SQL**
```sql
SELECT u.region, SUM(o.total_amount) AS total_spent
FROM users u
JOIN orders o ON u.user_id = o.user_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY u.region
ORDER BY total_spent DESC
```

**輸出說明**
- 4 筆結果（north、south、east、west）
- 排除 pending 和 cancelled 訂單（未完成付款）

---

## 範例 5：三表 JOIN（銷售排行）

**自然語言輸入**
```
最近銷售額最高的前 5 件商品是什麼？
```

**預期路由**
```json
{
  "tables": ["products", "orders", "order_items"],
  "confidence": 0.91,
  "reasoning": "需要 products 取商品名、order_items 取銷售數量和價格、orders 篩選有效訂單"
}
```

**預期 SQL**
```sql
SELECT p.name, p.category,
       SUM(oi.quantity * oi.unit_price) AS revenue,
       SUM(oi.quantity) AS total_sold
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY p.product_id, p.name, p.category
ORDER BY revenue DESC
LIMIT 5
```

**輸出說明**
- 5 筆結果，顯示商品名、品類、總銷售額、總售出數量
- 只計算已付款的訂單（排除 pending/cancelled）
- 注意使用 `oi.unit_price`（下單時快照）而非 `p.price`（當前售價）

---

## 範例 6：四表完整 JOIN（用戶購買明細）

**自然語言輸入**
```
bob 最近購買了哪些商品？
```

**預期路由**
```json
{
  "tables": ["users", "orders", "order_items", "products"],
  "confidence": 0.87,
  "reasoning": "需要 users 找 bob、orders 找訂單、order_items 找明細、products 取商品名"
}
```

**預期 SQL**
```sql
SELECT p.name, p.category, oi.quantity, oi.unit_price,
       oi.quantity * oi.unit_price AS subtotal,
       o.status, o.created_at
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE u.username = 'bob'
ORDER BY o.created_at DESC
```

**輸出說明**
- 顯示 bob 所有訂單的明細（含商品名、數量、小計、訂單狀態）
- 依下單時間降序排列
- 若 bob 沒有訂單，回傳空結果集（row_count = 0）

---

## 常見問題排查

### Claude 路由到錯誤的表
- 確認 `schema_registry.py` 中該表的 `description` 和 `example_queries` 夠清晰
- 可在 `ROUTER_SYSTEM_TEMPLATE`（`router/prompts.py`）中加入更多指引

### 生成的 SQL 有語法錯誤
- 確認 `GENERATOR_SYSTEM_TEMPLATE` 明確說明 SQLite 特有語法（如 `datetime()`、`strftime()`）
- schema_context 中的 DDL 必須正確（`schema_registry.py` 的 `ddl` 欄位）

### UnsafeSQLError
- Claude 有時生成 `WITH ... AS (SELECT ...)` CTE，這是合法 SELECT，不會觸發
- 若觸發是因為 Claude 生成了修改語句，檢查 Generator prompt 是否清楚說明只允許 SELECT
