"""DuckDB execution path: read Parquet files from MinIO, return ExecutionResult."""
import io
import logging
import os
from datetime import datetime
from typing import Dict, List, Optional
from uuid import uuid4

import duckdb
import pandas as pd

from text_to_sql.models.response import ExecutionResult

logger = logging.getLogger(__name__)

CSV_THRESHOLD = 50


class DuckDBExecutor:
    """Execute SQL queries via DuckDB, reading Parquet data directly from MinIO.

    For result sets with more than CSV_THRESHOLD rows the result is uploaded as
    a CSV file to MinIO and ``ExecutionResult.csv_url`` is set; ``rows`` will be
    empty in that case. Smaller result sets are returned inline.

    Args:
        minio_client: MinIO client instance (minio.Minio).
        bucket: MinIO bucket that holds the Parquet data files.
        table_names: All table names available in the bucket.
        conn: Optional pre-built DuckDB connection for testing. When provided,
              data must already be registered as views/tables on that connection
              and no S3 access is attempted.
    """

    def __init__(
        self,
        minio_client,
        bucket: str,
        table_names: List[str],
        conn: Optional[duckdb.DuckDBPyConnection] = None,
    ):
        self._minio_client = minio_client
        self._bucket = bucket
        self._table_names = table_names
        self._conn = conn  # injected for tests; None = use S3 path
        self._persistent_conn: Optional[duckdb.DuckDBPyConnection] = None

    # ------------------------------------------------------------------
    # Public interface
    # ------------------------------------------------------------------

    def execute(self, sql: str, table_refs: List[str]) -> ExecutionResult:
        """Execute *sql* and return an ExecutionResult.

        If the injected connection is available the query runs directly on it
        (test path, no MinIO). Otherwise S3 paths are constructed and Parquet
        files are mounted as DuckDB views before execution.

        Args:
            sql: The SQL query to execute.
            table_refs: Table names referenced in the SQL.

        Returns:
            ExecutionResult with inline rows (<=50) or csv_url (>50).
        """
        try:
            if self._conn is not None:
                df = self._conn.execute(sql).df()
            else:
                s3_paths = {t: f"s3://{self._bucket}/{t}.parquet" for t in table_refs}
                df = self._execute_in_duckdb(sql, s3_paths)
        except Exception as exc:
            logger.error("[DuckDBExecutor] query failed: %s", exc)
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
            self._persistent_conn = self._create_s3_conn()
        return self._persistent_conn

    def _create_s3_conn(self) -> duckdb.DuckDBPyConnection:
        """Create a DuckDB connection pre-configured with httpfs and S3 credentials."""
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

        conn = duckdb.connect()
        conn.execute("INSTALL httpfs; LOAD httpfs;")
        conn.execute(f"SET s3_endpoint='{endpoint}';")
        conn.execute(f"SET s3_access_key_id='{access_key}';")
        conn.execute(f"SET s3_secret_access_key='{secret_key}';")
        conn.execute("SET s3_use_ssl=false;")
        conn.execute("SET s3_url_style='path';")
        return conn

    def _execute_in_duckdb(self, sql: str, s3_paths: Dict[str, str]) -> pd.DataFrame:
        """Mount MinIO Parquet views on the persistent connection and execute *sql*."""
        conn = self._get_or_create_conn()

        for table_name, s3_path in s3_paths.items():
            conn.execute(
                f"CREATE OR REPLACE VIEW {table_name} AS SELECT * FROM read_parquet('{s3_path}');"
            )
            logger.debug("[DuckDBExecutor] Mounted %s → %s", table_name, s3_path)

        result = conn.execute(sql).df()
        logger.info("[DuckDBExecutor] executed SQL, %d rows", len(result))
        return result

    def _build_result(self, df: pd.DataFrame) -> ExecutionResult:
        columns = list(df.columns)
        row_count = len(df)

        if row_count > CSV_THRESHOLD:
            csv_url = self._upload_csv(df)
            logger.info(
                "[DuckDBExecutor] result has %d rows (>%d), uploaded CSV: %s",
                row_count,
                CSV_THRESHOLD,
                csv_url,
            )
            return ExecutionResult(
                success=True,
                columns=columns,
                rows=[],
                row_count=row_count,
                csv_url=csv_url,
            )

        rows = df.values.tolist()
        logger.info("[DuckDBExecutor] result has %d rows (inline)", row_count)
        return ExecutionResult(
            success=True,
            columns=columns,
            rows=rows,
            row_count=row_count,
        )

    def _upload_csv(self, df: pd.DataFrame) -> str:
        """Serialise *df* as CSV and upload to MinIO; return the object key."""
        key = f"results/{datetime.utcnow().strftime('%Y%m%d_%H%M%S')}_{uuid4().hex[:8]}.csv"
        buf = io.BytesIO(df.to_csv(index=False).encode("utf-8"))
        data_len = buf.getbuffer().nbytes

        self._minio_client.put_object(
            bucket_name=self._bucket,
            object_name=key,
            data=buf,
            length=data_len,
            content_type="text/csv",
        )
        logger.info("[DuckDBExecutor] Uploaded CSV → %s (%d bytes)", key, data_len)
        return key
