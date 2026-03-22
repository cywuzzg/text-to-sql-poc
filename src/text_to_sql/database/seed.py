import random
import sqlite3
from datetime import datetime, timedelta
from typing import List, Tuple

USERS: List[Tuple] = [
    ("alice", "alice@example.com", "north"),
    ("bob", "bob@example.com", "south"),
    ("carol", "carol@example.com", "east"),
    ("dave", "dave@example.com", "west"),
    ("eve", "eve@example.com", "north"),
    ("frank", "frank@example.com", "south"),
    ("grace", "grace@example.com", "east"),
    ("henry", "henry@example.com", "west"),
    ("iris", "iris@example.com", "north"),
    ("jack", "jack@example.com", "south"),
    ("karen", "karen@example.com", "east"),
    ("leo", "leo@example.com", "west"),
    ("mary", "mary@example.com", "north"),
    ("nick", "nick@example.com", "south"),
    ("olivia", "olivia@example.com", "east"),
    ("peter", "peter@example.com", "west"),
    ("quinn", "quinn@example.com", "north"),
    ("rose", "rose@example.com", "south"),
    ("sam", "sam@example.com", "east"),
    ("tina", "tina@example.com", "west"),
]

PRODUCTS: List[Tuple] = [
    ("iPhone 15", "electronics", 29900.0, 50),
    ("Samsung Galaxy S24", "electronics", 24900.0, 30),
    ("MacBook Air M3", "electronics", 42900.0, 20),
    ("AirPods Pro", "electronics", 7490.0, 100),
    ("iPad Mini", "electronics", 15900.0, 40),
    ("Sony WH-1000XM5", "electronics", 9900.0, 60),
    ("Nintendo Switch", "electronics", 9980.0, 35),
    ("Kindle Paperwhite", "electronics", 4490.0, 80),
    ("夏季棉T-恤（白）", "clothing", 299.0, 200),
    ("牛仔褲（深藍）", "clothing", 890.0, 150),
    ("運動外套", "clothing", 1290.0, 90),
    ("連帽上衣", "clothing", 690.0, 120),
    ("休閒短褲", "clothing", 490.0, 180),
    ("polo衫", "clothing", 590.0, 100),
    ("風衣", "clothing", 2490.0, 45),
    ("運動鞋", "clothing", 1890.0, 70),
    ("有機米（5kg）", "food", 350.0, 300),
    ("橄欖油（500ml）", "food", 280.0, 200),
    ("黑巧克力禮盒", "food", 450.0, 150),
    ("綠茶茶葉（100g）", "food", 380.0, 200),
    ("咖啡豆（250g）", "food", 520.0, 180),
    ("蜂蜜（500g）", "food", 290.0, 250),
    ("果乾綜合包", "food", 199.0, 300),
    ("燕麥片（1kg）", "food", 159.0, 400),
    ("北歐風桌燈", "home", 1290.0, 60),
    ("記憶棉枕頭", "home", 890.0, 80),
    ("香氛蠟燭組", "home", 590.0, 100),
    ("收納盒六件組", "home", 490.0, 120),
    ("竹製砧板", "home", 350.0, 150),
    ("不鏽鋼保溫瓶", "home", 680.0, 200),
]

_STATUSES = ["pending", "paid", "shipped", "delivered", "cancelled"]
_STATUS_WEIGHTS = [0.1, 0.2, 0.3, 0.35, 0.05]


def _random_date(days_ago_max: int = 90) -> str:
    delta = timedelta(days=random.randint(0, days_ago_max))
    return (datetime.now() - delta).strftime("%Y-%m-%d %H:%M:%S")


def seed(conn: sqlite3.Connection, seed_value: int = 42) -> None:
    """Insert reproducible seed data into all tables."""
    random.seed(seed_value)
    cur = conn.cursor()

    # users
    cur.executemany(
        "INSERT OR IGNORE INTO users (username, email, region) VALUES (?, ?, ?)",
        USERS,
    )

    # products
    cur.executemany(
        "INSERT OR IGNORE INTO products (name, category, price, stock) VALUES (?, ?, ?, ?)",
        PRODUCTS,
    )

    conn.commit()
    user_ids = [row[0] for row in cur.execute("SELECT user_id FROM users").fetchall()]
    product_ids = [row[0] for row in cur.execute("SELECT product_id FROM products").fetchall()]
    product_prices = {
        row[0]: row[1]
        for row in cur.execute("SELECT product_id, price FROM products").fetchall()
    }

    # orders + order_items  (60 orders, ~2 items each)
    for _ in range(60):
        uid = random.choice(user_ids)
        status = random.choices(_STATUSES, weights=_STATUS_WEIGHTS)[0]
        created = _random_date()

        # pick 1-3 items
        items_count = random.randint(1, 3)
        chosen_products = random.sample(product_ids, items_count)
        quantities = [random.randint(1, 4) for _ in chosen_products]
        prices = [product_prices[pid] for pid in chosen_products]
        total = sum(q * p for q, p in zip(quantities, prices))

        cur.execute(
            "INSERT INTO orders (user_id, status, total_amount, created_at) VALUES (?, ?, ?, ?)",
            (uid, status, total, created),
        )
        order_id = cur.lastrowid

        for pid, qty, price in zip(chosen_products, quantities, prices):
            cur.execute(
                "INSERT INTO order_items (order_id, product_id, quantity, unit_price) VALUES (?, ?, ?, ?)",
                (order_id, pid, qty, price),
            )

    conn.commit()
