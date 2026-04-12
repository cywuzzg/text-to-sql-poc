"""DuckDB file executor — reads directly from a persistent .duckdb file.

Unlike LocalDuckDBExecutor (which mounts Parquet views), this executor
connects to a DuckDB file where tables already exist as native tables.

Result sets exceeding CSV_THRESHOLD rows are saved as CSV files instead of
being returned inline.
"""
import logging
from datetime import datetime
from pathlib import Path
from typing import List, Optional
from uuid import uuid4

import duckdb
import pandas as pd

from text_to_sql.models.response import ExecutionResult

logger = logging.getLogger(__name__)

CSV_THRESHOLD = 50


class DuckDBFileExecutor:
    """Execute SQL queries against a persistent DuckDB file.

    Args:
        db_path: Path to the .duckdb file. Must exist before calling execute().
        conn: Optional pre-built DuckDB connection for testing.
        results_dir: Directory for saving large CSV results. Defaults to a
                     ``results/`` sub-directory next to the .duckdb file.
    """

    def __init__(
        self,
        db_path,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
        results_dir=None,
    ):
        self._db_path = Path(db_path)
        self._conn = conn
        self._results_dir = Path(results_dir) if results_dir else self._db_path.parent / "results"
        self._persistent_conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def execute(self, sql: str, table_refs: List[str]) -> ExecutionResult:
        """Execute *sql* against the DuckDB file and return an ExecutionResult.

        Args:
            sql: The SQL query to execute.
            table_refs: Table names referenced in the query. Kept for API
                        compatibility with other executors but not used here
                        (tables are real tables in the DuckDB file).

        Returns:
            ExecutionResult with inline rows (<=CSV_THRESHOLD) or a local
            csv_url (>CSV_THRESHOLD).
        """
        try:
            if self._conn is not None:
                df = self._conn.execute(sql).df()
            else:
                df = self._execute_from_file(sql)
        except Exception as exc:
            logger.error("[DuckDBFileExecutor] query failed: %s", exc)
            return ExecutionResult(
                success=False,
                columns=[],
                rows=[],
                row_count=0,
                error=str(exc),
            )

        return self._build_result(df)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _get_or_create_conn(self) -> duckdb.DuckDBPyConnection:
        if self._persistent_conn is None:
            self._persistent_conn = duckdb.connect(str(self._db_path))
        return self._persistent_conn

    def _execute_from_file(self, sql: str) -> pd.DataFrame:
        conn = self._get_or_create_conn()
        result = conn.execute(sql).df()
        logger.info("[DuckDBFileExecutor] executed SQL, %d rows", len(result))
        return result

    def _build_result(self, df: pd.DataFrame) -> ExecutionResult:
        columns = list(df.columns)
        row_count = len(df)

        if row_count > CSV_THRESHOLD:
            csv_path = self._save_csv(df)
            logger.info(
                "[DuckDBFileExecutor] result has %d rows (>%d), saved CSV: %s",
                row_count,
                CSV_THRESHOLD,
                csv_path,
            )
            return ExecutionResult(
                success=True,
                columns=columns,
                rows=[],
                row_count=row_count,
                csv_url=str(csv_path),
            )

        logger.info("[DuckDBFileExecutor] result has %d rows (inline)", row_count)
        return ExecutionResult(
            success=True,
            columns=columns,
            rows=df.values.tolist(),
            row_count=row_count,
        )

    def _save_csv(self, df: pd.DataFrame) -> Path:
        self._results_dir.mkdir(parents=True, exist_ok=True)
        filename = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.csv"
        csv_path = (self._results_dir / filename).resolve()
        df.to_csv(csv_path, index=False)
        logger.info("[DuckDBFileExecutor] Saved CSV → %s", csv_path)
        return csv_path
