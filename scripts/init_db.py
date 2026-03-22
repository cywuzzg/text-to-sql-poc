"""Initialize the SQLite database with schema and seed data."""
import sqlite3
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent.parent / "src"))

from text_to_sql.config import DB_PATH
from text_to_sql.database.schema import DDL_STATEMENTS
from text_to_sql.database.seed import seed


def init_db(db_path: str = DB_PATH) -> None:
    Path(db_path).parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(db_path)
    conn.execute("PRAGMA foreign_keys = ON")
    for stmt in DDL_STATEMENTS:
        conn.execute(stmt)
    conn.commit()
    seed(conn)
    conn.close()
    print(f"Database initialized at: {db_path}")


if __name__ == "__main__":
    db_path = sys.argv[1] if len(sys.argv) > 1 else DB_PATH
    init_db(db_path)
