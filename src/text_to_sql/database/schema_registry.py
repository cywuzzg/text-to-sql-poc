from dataclasses import dataclass, field
from typing import Dict, List


@dataclass
class ColumnInfo:
    name: str
    type: str
    description: str


@dataclass
class TableSchema:
    name: str
    description: str
    columns: List[ColumnInfo]
    ddl: str
    example_queries: List[str] = field(default_factory=list)


_REGISTRY: Dict[str, TableSchema] = {
    "users": TableSchema(
        name="users",
        description="電商平台用戶資料，包含帳號、Email、所屬地區",
        columns=[
            ColumnInfo("user_id", "INTEGER", "主鍵，自動遞增"),
            ColumnInfo("username", "TEXT", "用戶名，唯一"),
            ColumnInfo("email", "TEXT", "電子郵件，唯一"),
            ColumnInfo("created_at", "TEXT", "帳號建立時間（ISO 8601）"),
            ColumnInfo("region", "TEXT", "所屬地區：north / south / east / west"),
        ],
        ddl=(
            "CREATE TABLE users (\n"
            "    user_id    INTEGER PRIMARY KEY,\n"
            "    username   VARCHAR NOT NULL UNIQUE,\n"
            "    email      VARCHAR NOT NULL UNIQUE,\n"
            "    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,\n"
            "    region     VARCHAR CHECK(region IN ('north','south','east','west'))\n"
            ")"
        ),
        example_queries=["查詢某地區所有用戶", "查某用戶的基本資料", "哪個地區用戶最多"],
    ),
    "products": TableSchema(
        name="products",
        description="商品資料，包含名稱、品類、價格、庫存數量",
        columns=[
            ColumnInfo("product_id", "INTEGER", "主鍵，自動遞增"),
            ColumnInfo("name", "TEXT", "商品名稱"),
            ColumnInfo("category", "TEXT", "品類：electronics / clothing / food / home"),
            ColumnInfo("price", "REAL", "售價（元）"),
            ColumnInfo("stock", "INTEGER", "庫存數量"),
            ColumnInfo("created_at", "TEXT", "上架時間（ISO 8601）"),
        ],
        ddl=(
            "CREATE TABLE products (\n"
            "    product_id INTEGER PRIMARY KEY,\n"
            "    name       VARCHAR NOT NULL,\n"
            "    category   VARCHAR NOT NULL CHECK(category IN ('electronics','clothing','food','home')),\n"
            "    price      DOUBLE NOT NULL CHECK(price > 0),\n"
            "    stock      INTEGER NOT NULL DEFAULT 0 CHECK(stock >= 0),\n"
            "    created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ")"
        ),
        example_queries=["庫存不足的商品", "某品類所有商品", "價格最高的前 10 件商品"],
    ),
    "orders": TableSchema(
        name="orders",
        description="訂單主表，記錄每筆訂單的用戶、狀態、總金額",
        columns=[
            ColumnInfo("order_id", "INTEGER", "主鍵，自動遞增"),
            ColumnInfo("user_id", "INTEGER", "外鍵，關聯 users.user_id"),
            ColumnInfo(
                "status",
                "TEXT",
                "訂單狀態：pending / paid / shipped / delivered / cancelled",
            ),
            ColumnInfo("total_amount", "REAL", "訂單總金額（元）"),
            ColumnInfo("created_at", "TEXT", "下單時間（ISO 8601）"),
        ],
        ddl=(
            "CREATE TABLE orders (\n"
            "    order_id     INTEGER PRIMARY KEY,\n"
            "    user_id      INTEGER NOT NULL REFERENCES users(user_id),\n"
            "    status       VARCHAR NOT NULL CHECK(status IN ('pending','paid','shipped','delivered','cancelled')),\n"
            "    total_amount DOUBLE NOT NULL CHECK(total_amount >= 0),\n"
            "    created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP\n"
            ")"
        ),
        example_queries=["某用戶所有訂單", "已完成訂單總金額", "最近 30 天訂單數量"],
    ),
    "order_items": TableSchema(
        name="order_items",
        description="訂單明細表，記錄每筆訂單包含的商品、數量、下單時快照價格",
        columns=[
            ColumnInfo("item_id", "INTEGER", "主鍵，自動遞增"),
            ColumnInfo("order_id", "INTEGER", "外鍵，關聯 orders.order_id"),
            ColumnInfo("product_id", "INTEGER", "外鍵，關聯 products.product_id"),
            ColumnInfo("quantity", "INTEGER", "購買數量"),
            ColumnInfo("unit_price", "REAL", "下單時的商品價格快照（元）"),
        ],
        ddl=(
            "CREATE TABLE order_items (\n"
            "    item_id    INTEGER PRIMARY KEY,\n"
            "    order_id   INTEGER NOT NULL REFERENCES orders(order_id),\n"
            "    product_id INTEGER NOT NULL REFERENCES products(product_id),\n"
            "    quantity   INTEGER NOT NULL CHECK(quantity > 0),\n"
            "    unit_price DOUBLE NOT NULL CHECK(unit_price > 0)\n"
            ")"
        ),
        example_queries=["某訂單的所有商品", "銷售量最高的商品", "各商品的總銷售額"],
    ),
}


def get_all_table_names() -> List[str]:
    return list(_REGISTRY.keys())


def get_table_schema(table_name: str) -> TableSchema:
    if table_name not in _REGISTRY:
        raise KeyError(f"Unknown table: '{table_name}'. Available: {list(_REGISTRY.keys())}")
    return _REGISTRY[table_name]


def get_schema_summary_for_routing() -> str:
    """Return a concise summary of all tables for the Table Router prompt."""
    lines = ["可用資料表：\n"]
    for ts in _REGISTRY.values():
        key_cols = ", ".join(c.name for c in ts.columns[:4])
        lines.append(f"- {ts.name}: {ts.description}（主要欄位：{key_cols}）")
    return "\n".join(lines)


def get_schema_detail_for_generation(tables: List[str]) -> str:
    """Return full DDL + column descriptions for the given tables."""
    parts = []
    for table_name in tables:
        ts = get_table_schema(table_name)
        col_desc = "\n".join(
            f"  - {c.name} ({c.type}): {c.description}" for c in ts.columns
        )
        parts.append(f"### {ts.name}\n{ts.ddl}\n\n欄位說明：\n{col_desc}")
    return "\n\n".join(parts)
