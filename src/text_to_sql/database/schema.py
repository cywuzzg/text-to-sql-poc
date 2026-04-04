DDL_STATEMENTS = [
    """
    CREATE TABLE IF NOT EXISTS users (
        user_id    INTEGER PRIMARY KEY,
        username   VARCHAR NOT NULL UNIQUE,
        email      VARCHAR NOT NULL UNIQUE,
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP,
        region     VARCHAR CHECK(region IN ('north','south','east','west'))
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS products (
        product_id INTEGER PRIMARY KEY,
        name       VARCHAR NOT NULL,
        category   VARCHAR NOT NULL CHECK(category IN ('electronics','clothing','food','home')),
        price      DOUBLE NOT NULL CHECK(price > 0),
        stock      INTEGER NOT NULL DEFAULT 0 CHECK(stock >= 0),
        created_at TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS orders (
        order_id     INTEGER PRIMARY KEY,
        user_id      INTEGER NOT NULL REFERENCES users(user_id),
        status       VARCHAR NOT NULL CHECK(status IN ('pending','paid','shipped','delivered','cancelled')),
        total_amount DOUBLE NOT NULL CHECK(total_amount >= 0),
        created_at   TIMESTAMP NOT NULL DEFAULT CURRENT_TIMESTAMP
    )
    """,
    """
    CREATE TABLE IF NOT EXISTS order_items (
        item_id    INTEGER PRIMARY KEY,
        order_id   INTEGER NOT NULL REFERENCES orders(order_id),
        product_id INTEGER NOT NULL REFERENCES products(product_id),
        quantity   INTEGER NOT NULL CHECK(quantity > 0),
        unit_price DOUBLE NOT NULL CHECK(unit_price > 0)
    )
    """,
]
