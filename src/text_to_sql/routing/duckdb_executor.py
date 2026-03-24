"""DuckDB execution path: export tables to MinIO, then query via DuckDB."""
import io
import logging
import os
import sqlite3
from typing import Dict, List, Optional

import duckdb
import pandas as pd

logger = logging.getLogger(__name__)


class DuckDBExecutor:
    """Execute heavy SQL queries via DuckDB, reading data from MinIO parquet files.

    Workflow:
    1. Export relevant tables from the main DB (SQLite) to parquet files on MinIO.
    2. Mount the parquet files as DuckDB views (named after the original tables).
    3. Execute the SQL as-is within DuckDB and return a DataFrame.
    """

    def __init__(
        self,
        minio_client,
        bucket: str,
        db_path: str = "",
        conn: Optional[sqlite3.Connection] = None,
    ):
        self._minio_client = minio_client
        self._bucket = bucket
        self._db_path = db_path
        self._conn = conn  # injected connection for tests

    def _get_sqlite_conn(self) -> sqlite3.Connection:
        if self._conn is not None:
            return self._conn
        conn = sqlite3.connect(self._db_path)
        conn.execute("PRAGMA foreign_keys = ON")
        return conn

    def export_table_to_minio(self, table_name: str) -> str:
        """Export a table from the main DB to MinIO as parquet.

        Args:
            table_name: Name of the table to export.

        Returns:
            The MinIO S3 path: ``s3://<bucket>/<table_name>.parquet``
        """
        managed = self._conn is None
        conn = self._get_sqlite_conn()
        try:
            df = pd.read_sql_query(f"SELECT * FROM {table_name}", conn)
        finally:
            if managed:
                conn.close()

        buffer = io.BytesIO()
        df.to_parquet(buffer, index=False, engine="pyarrow")
        buffer.seek(0)
        data_len = buffer.getbuffer().nbytes

        object_name = f"{table_name}.parquet"
        self._minio_client.put_object(
            bucket_name=self._bucket,
            object_name=object_name,
            data=buffer,
            length=data_len,
            content_type="application/octet-stream",
        )

        path = f"s3://{self._bucket}/{object_name}"
        logger.info("[DuckDBExecutor] Exported %s → %s (%d bytes)", table_name, path, data_len)
        return path

    def execute(self, sql: str, table_refs: List[str]) -> pd.DataFrame:
        """Execute SQL in DuckDB using parquet files from MinIO.

        Args:
            sql: The SQL query to execute (table names must match table_refs).
            table_refs: List of table names referenced in the SQL.

        Returns:
            Query result as a pandas DataFrame.
        """
        s3_paths: Dict[str, str] = {}
        for table_name in table_refs:
            s3_paths[table_name] = self.export_table_to_minio(table_name)

        return self._execute_in_duckdb(sql, s3_paths)

    def _execute_in_duckdb(self, sql: str, s3_paths: Dict[str, str]) -> pd.DataFrame:
        """Internal: mount parquet files as views and execute SQL in DuckDB.

        Args:
            sql: SQL to execute.
            s3_paths: Mapping from table name to S3 parquet path.

        Returns:
            Query result as a pandas DataFrame.
        """
        endpoint = os.environ.get("MINIO_ENDPOINT", "localhost:9000")
        access_key = os.environ.get("MINIO_ACCESS_KEY", "minioadmin")
        secret_key = os.environ.get("MINIO_SECRET_KEY", "minioadmin")

        with duckdb.connect() as conn:
            conn.execute("INSTALL httpfs; LOAD httpfs;")
            conn.execute(f"SET s3_endpoint='{endpoint}';")
            conn.execute(f"SET s3_access_key_id='{access_key}';")
            conn.execute(f"SET s3_secret_access_key='{secret_key}';")
            conn.execute("SET s3_use_ssl=false;")
            conn.execute("SET s3_url_style='path';")

            for table_name, s3_path in s3_paths.items():
                conn.execute(
                    f"CREATE VIEW {table_name} AS SELECT * FROM read_parquet('{s3_path}');"
                )
                logger.debug("[DuckDBExecutor] Mounted %s → %s", table_name, s3_path)

            result = conn.execute(sql).df()

        logger.info("[DuckDBExecutor] Executed SQL, returned %d rows", len(result))
        return result
