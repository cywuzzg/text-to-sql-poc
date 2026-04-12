import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL: str = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")

MINIO_ENDPOINT: str = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY: str = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY: str = os.environ.get("MINIO_SECRET_KEY", "minioadmin")
MINIO_BUCKET: str = os.environ.get("MINIO_BUCKET", "text-to-sql")

PROJECT_ROOT = Path(__file__).parent.parent.parent

DB_PATH: str = os.environ.get("DB_PATH", str(PROJECT_ROOT / "database" / "ecommerce.db"))
DUCKDB_PATH: str = os.environ.get("DUCKDB_PATH", str(PROJECT_ROOT / "database" / "ecommerce.duckdb"))
