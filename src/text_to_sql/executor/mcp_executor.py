"""
Executor layer.

DirectSQLiteExecutor  — uses sqlite3 directly (fallback / tests)
MCPExecutor           — uses mcp-server-sqlite via stdio transport
"""
import asyncio
import sqlite3
from typing import Any, List

from text_to_sql.models.response import ExecutionResult


class DirectSQLiteExecutor:
    """Execute SQL directly via sqlite3. Used for tests and as a fallback."""

    def __init__(self, db_path: str = "", conn: sqlite3.Connection = None):
        self._db_path = db_path
        self._conn = conn  # injected connection (e.g. in-memory for tests)

    def _get_conn(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def execute(self, sql: str) -> ExecutionResult:
        managed = self._conn is None
        conn = self._get_conn()
        try:
            cur = conn.execute(sql)
            columns: List[str] = [d[0] for d in (cur.description or [])]
            rows: List[List[Any]] = [list(row) for row in cur.fetchall()]
            return ExecutionResult(
                success=True,
                columns=columns,
                rows=rows,
                row_count=len(rows),
            )
        except Exception as exc:
            return ExecutionResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=str(exc),
            )
        finally:
            if managed:
                conn.close()


class MCPExecutor:
    """Execute SQL via mcp-server-sqlite (stdio transport)."""

    def __init__(self, db_path: str):
        self._db_path = db_path

    def execute(self, sql: str) -> ExecutionResult:
        return asyncio.run(self._aexecute(sql))

    async def _aexecute(self, sql: str) -> ExecutionResult:
        from mcp import ClientSession, StdioServerParameters
        from mcp.client.stdio import stdio_client

        server_params = StdioServerParameters(
            command="uvx",
            args=["mcp-server-sqlite", "--db-path", self._db_path],
        )
        try:
            async with stdio_client(server_params) as (read, write):
                async with ClientSession(read, write) as session:
                    await session.initialize()
                    result = await session.call_tool(
                        "read_query", arguments={"query": sql}
                    )
                    return self._parse_mcp_result(result)
        except Exception as exc:
            return ExecutionResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=str(exc),
            )

    @staticmethod
    def _parse_mcp_result(result) -> ExecutionResult:
        """Parse the CallToolResult from mcp-server-sqlite."""
        import json

        try:
            # mcp-server-sqlite returns content as a list of TextContent
            raw = result.content[0].text if result.content else "[]"
            data = json.loads(raw)

            if not data:
                return ExecutionResult(success=True, columns=[], rows=[], row_count=0)

            if isinstance(data, list) and isinstance(data[0], dict):
                columns = list(data[0].keys())
                rows = [list(item.values()) for item in data]
                return ExecutionResult(
                    success=True,
                    columns=columns,
                    rows=rows,
                    row_count=len(rows),
                )
            return ExecutionResult(success=True, columns=[], rows=data, row_count=len(data))
        except Exception as exc:
            return ExecutionResult(
                success=False, columns=[], rows=[], row_count=0, error=str(exc)
            )
