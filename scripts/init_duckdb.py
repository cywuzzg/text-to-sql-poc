"""Initialize the local DuckDB database with seed data.

Creates (or re-seeds) database/ecommerce.duckdb with the four e-commerce tables.
Safe to run multiple times — existing rows are cleared before re-inserting.

Usage:
    python scripts/init_duckdb.py
    DUCKDB_PATH=/custom/path.duckdb python scripts/init_duckdb.py
"""
import sys
from pathlib import Path

# Make sure the src package is importable when run directly
sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from text_to_sql.config import DUCKDB_PATH
from text_to_sql.database.seed import seed_duckdb

if __name__ == "__main__":
    print(f"Initializing DuckDB database at: {DUCKDB_PATH}")
    seed_duckdb(DUCKDB_PATH)
    print("Done. Tables: users, products, orders, order_items")
    print(f"  → {Path(DUCKDB_PATH).stat().st_size / 1024:.1f} KB")
