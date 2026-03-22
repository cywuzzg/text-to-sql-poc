import os
from pathlib import Path

from dotenv import load_dotenv

load_dotenv()

ANTHROPIC_API_KEY: str = os.environ.get("ANTHROPIC_API_KEY", "")
CLAUDE_MODEL: str = os.environ.get("CLAUDE_MODEL", "claude-haiku-4-5-20251001")
DB_PATH: str = os.environ.get("DB_PATH", "database/ecommerce.db")

PROJECT_ROOT = Path(__file__).parent.parent.parent
