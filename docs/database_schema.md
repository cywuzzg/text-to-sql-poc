# 資料庫 Schema 設計

## 概覽

電商場景，4 張核心表，存放於 `database/ecommerce.db`（SQLite）。

```
users ──< orders ──< order_items >── products
```

| 表 | 說明 | 種子資料量 |
|---|---|---|
| users | 電商平台用戶 | 20 筆 |
| products | 商品目錄 | 30 筆 |
| orders | 訂單主表 | 60 筆 |
| order_items | 訂單明細（含快照價格） | ~125 筆 |

---

## 完整 DDL

### users

```sql
CREATE TABLE users (
    user_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    username   TEXT NOT NULL UNIQUE,
    email      TEXT NOT NULL UNIQUE,
    created_at TEXT NOT NULL DEFAULT (datetime('now')),
    region     TEXT CHECK(region IN ('north','south','east','west'))
);
```

| 欄位 | 型別 | 說明 |
|---|---|---|
| user_id | INTEGER | 主鍵，自動遞增 |
| username | TEXT | 用戶名（唯一），如 alice, bob |
| email | TEXT | 電子郵件（唯一） |
| created_at | TEXT | 帳號建立時間，ISO 8601 格式 |
| region | TEXT | 所屬地區：`north` / `south` / `east` / `west` |

---

### products

```sql
CREATE TABLE products (
    product_id INTEGER PRIMARY KEY AUTOINCREMENT,
    name       TEXT NOT NULL,
    category   TEXT NOT NULL CHECK(category IN ('electronics','clothing','food','home')),
    price      REAL NOT NULL CHECK(price > 0),
    stock      INTEGER NOT NULL DEFAULT 0 CHECK(stock >= 0),
    created_at TEXT NOT NULL DEFAULT (datetime('now'))
);
```

| 欄位 | 型別 | 說明 |
|---|---|---|
| product_id | INTEGER | 主鍵，自動遞增 |
| name | TEXT | 商品名稱（中文），如「iPhone 15」、「有機米（5kg）」 |
| category | TEXT | 品類：`electronics` / `clothing` / `food` / `home` |
| price | REAL | 售價（元），必須 > 0 |
| stock | INTEGER | 庫存數量，必須 ≥ 0 |
| created_at | TEXT | 上架時間，ISO 8601 格式 |

**品類分布**：electronics 8 件、clothing 8 件、food 8 件、home 6 件

---

### orders

```sql
CREATE TABLE orders (
    order_id     INTEGER PRIMARY KEY AUTOINCREMENT,
    user_id      INTEGER NOT NULL REFERENCES users(user_id),
    status       TEXT NOT NULL CHECK(status IN ('pending','paid','shipped','delivered','cancelled')),
    total_amount REAL NOT NULL CHECK(total_amount >= 0),
    created_at   TEXT NOT NULL DEFAULT (datetime('now'))
);
```

| 欄位 | 型別 | 說明 |
|---|---|---|
| order_id | INTEGER | 主鍵，自動遞增 |
| user_id | INTEGER | 外鍵，關聯 users.user_id |
| status | TEXT | 訂單狀態（見下表） |
| total_amount | REAL | 訂單總金額（元），等於 Σ(quantity × unit_price) |
| created_at | TEXT | 下單時間，ISO 8601 格式，時間跨度約 90 天 |

**狀態分布（概率）**：

| 狀態 | 說明 | 種子概率 |
|---|---|---|
| pending | 待付款 | 10% |
| paid | 已付款 | 20% |
| shipped | 已出貨 | 30% |
| delivered | 已送達 | 35% |
| cancelled | 已取消 | 5% |

---

### order_items

```sql
CREATE TABLE order_items (
    item_id    INTEGER PRIMARY KEY AUTOINCREMENT,
    order_id   INTEGER NOT NULL REFERENCES orders(order_id),
    product_id INTEGER NOT NULL REFERENCES products(product_id),
    quantity   INTEGER NOT NULL CHECK(quantity > 0),
    unit_price REAL NOT NULL CHECK(unit_price > 0)
);
```

| 欄位 | 型別 | 說明 |
|---|---|---|
| item_id | INTEGER | 主鍵，自動遞增 |
| order_id | INTEGER | 外鍵，關聯 orders.order_id |
| product_id | INTEGER | 外鍵，關聯 products.product_id |
| quantity | INTEGER | 購買數量（1～4） |
| unit_price | REAL | **下單時**的商品價格快照（非當前售價） |

> `unit_price` 儲存下單當時的價格，與 products.price 可能不同（若後來調價）。計算銷售額應使用 `unit_price × quantity`，而非 `products.price`。

---

## 表間關聯

```
users
  user_id (PK)
    │
    └─< orders
          order_id (PK)
          user_id (FK → users.user_id)
            │
            └─< order_items
                  item_id (PK)
                  order_id (FK → orders.order_id)
                  product_id (FK → products.product_id)
                    │
products              │
  product_id (PK) ───┘
```

---

## 常用 JOIN 模式

### 查詢用戶的訂單（含商品）
```sql
SELECT u.username, o.order_id, p.name, oi.quantity, oi.unit_price
FROM users u
JOIN orders o ON u.user_id = o.user_id
JOIN order_items oi ON o.order_id = oi.order_id
JOIN products p ON oi.product_id = p.product_id
WHERE u.username = 'alice'
ORDER BY o.created_at DESC;
```

### 計算商品銷售額
```sql
SELECT p.name, SUM(oi.quantity * oi.unit_price) AS revenue
FROM products p
JOIN order_items oi ON p.product_id = oi.product_id
JOIN orders o ON oi.order_id = o.order_id
WHERE o.status IN ('paid', 'shipped', 'delivered')
GROUP BY p.product_id, p.name
ORDER BY revenue DESC;
```

---

## 新增一張表的步驟

1. **`src/text_to_sql/database/schema.py`**
   在 `DDL_STATEMENTS` 列表末尾新增 `CREATE TABLE IF NOT EXISTS ...`

2. **`src/text_to_sql/database/schema_registry.py`**
   在 `_REGISTRY` dict 新增 `TableSchema`（含 name、description、columns、ddl、example_queries）

3. **`src/text_to_sql/database/seed.py`**
   新增種子資料常數和 `seed()` 中的插入邏輯

4. **`tests/unit/test_schema_registry.py`**
   更新 `test_all_four_tables_exist` 的 assert（改成 5 張表）

5. **`docs/database_schema.md`**（本文件）
   新增新表的 DDL 和欄位說明

6. **`scripts/init_db.py`**（通常不需改）
   `DDL_STATEMENTS` 是列表，init_db 自動執行全部

```bash
# 重建資料庫驗證
python scripts/init_db.py
python -c "import sqlite3; conn=sqlite3.connect('database/ecommerce.db'); print(conn.execute('SELECT name FROM sqlite_master WHERE type=\"table\"').fetchall())"
```
