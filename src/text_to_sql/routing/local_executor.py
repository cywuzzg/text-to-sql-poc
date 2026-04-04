"""Local file-based DuckDB executor — no MinIO required.

Reads Parquet files from a local directory and executes SQL queries via DuckDB.
Result sets exceeding CSV_THRESHOLD rows are saved as CSV files in a ``results/``
sub-directory of *data_dir* instead of being uploaded to MinIO.
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


class LocalDuckDBExecutor:
    """Execute SQL queries via DuckDB, reading Parquet files from a local directory.

    This executor is meant for local development and testing when MinIO is not
    available. It behaves identically to ``DuckDBExecutor`` except:

    - Parquet data is read from ``{data_dir}/{table}.parquet``
    - Large results (>CSV_THRESHOLD rows) are written to
      ``{data_dir}/results/{timestamp}_{uuid}.csv`` and the absolute path is
      returned as ``ExecutionResult.csv_url``.

    Args:
        data_dir: Directory that contains the Parquet files (and will hold the
                  ``results/`` sub-directory).
        conn: Optional pre-built DuckDB connection for testing. When provided,
              data must already be registered on that connection and no file I/O
              is attempted for reading.
    """

    def __init__(
        self,
        data_dir,
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ):
        self._data_dir = Path(data_dir)
        self._conn = conn

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def execute(self, sql: str, table_refs: List[str]) -> ExecutionResult:
        """Execute *sql* and return an ExecutionResult.

        Uses the injected connection when available (test path). Otherwise mounts
        Parquet views from *data_dir* and executes in a fresh DuckDB connection.

        Args:
            sql: The SQL query to execute.
            table_refs: Table names referenced in the query (used to mount views).

        Returns:
            ExecutionResult with inline rows (<=CSV_THRESHOLD) or a local
            csv_url (>CSV_THRESHOLD).
        """
        try:
            if self._conn is not None:
                df = self._conn.execute(sql).df()
            else:
                df = self._execute_from_parquet(sql, table_refs)
        except Exception as exc:
            logger.error("[LocalDuckDBExecutor] query failed: %s", exc)
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

    def _build_result(self, df: pd.DataFrame) -> ExecutionResult:
        columns = list(df.columns)
        row_count = len(df)

        if row_count > CSV_THRESHOLD:
            csv_path = self._save_csv(df)
            logger.info(
                "[LocalDuckDBExecutor] result has %d rows (>%d), saved CSV: %s",
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

        logger.info("[LocalDuckDBExecutor] result has %d rows (inline)", row_count)
        return ExecutionResult(
            success=True,
            columns=columns,
            rows=df.values.tolist(),
            row_count=row_count,
        )

    def _save_csv(self, df: pd.DataFrame) -> Path:
        """Write *df* as CSV to ``{data_dir}/results/``; return the absolute path."""
        results_dir = self._data_dir / "results"
        results_dir.mkdir(parents=True, exist_ok=True)

        filename = f"{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.csv"
        csv_path = (results_dir / filename).resolve()
        df.to_csv(csv_path, index=False)
        logger.info("[LocalDuckDBExecutor] Saved CSV → %s", csv_path)
        return csv_path

    def _execute_from_parquet(self, sql: str, table_refs: List[str]) -> pd.DataFrame:
        """Mount local Parquet views and execute *sql* in a fresh DuckDB connection."""
        with duckdb.connect() as conn:
            for table_name in table_refs:
                parquet_path = self._data_dir / f"{table_name}.parquet"
                conn.execute(
                    f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{parquet_path}');"
                )
                logger.debug(
                    "[LocalDuckDBExecutor] Mounted %s → %s", table_name, parquet_path
                )

            result = conn.execute(sql).df()

        logger.info("[LocalDuckDBExecutor] executed SQL, %d rows", len(result))
        return result
