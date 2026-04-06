"""DB Exec runner — executes DML SQL statements with commit."""
from __future__ import annotations

import asyncio
import time
import urllib.parse
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout
from brix.runners.db_query import _detect_driver, _strip_sqlite_prefix


def _execute_sqlite(dsn: str, query: str, params: list | tuple | dict | None) -> int:
    import sqlite3

    path = _strip_sqlite_prefix(dsn)
    conn = sqlite3.connect(path)
    try:
        cur = conn.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        conn.commit()
        return cur.rowcount
    finally:
        conn.close()


def _execute_postgresql(dsn: str, query: str, params: list | tuple | dict | None) -> int:
    import psycopg2  # type: ignore

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor() as cur:
            if params is not None:
                cur.execute(query, params)
            else:
                cur.execute(query)
            affected_rows = cur.rowcount
        conn.commit()
        return affected_rows
    finally:
        conn.close()


def _execute_duckdb(dsn: str, query: str, params: list | tuple | dict | None) -> int:
    import duckdb  # type: ignore

    conn = duckdb.connect(dsn)
    try:
        cur = conn.cursor()
        if params is not None:
            cur.execute(query, params)
        else:
            cur.execute(query)
        affected_rows = cur.rowcount
        conn.commit()
        return affected_rows
    finally:
        conn.close()


def _execute_mysql(dsn: str, query: str, params: list | tuple | dict | None) -> int:
    import pymysql  # type: ignore

    parsed = urllib.parse.urlparse(dsn)
    conn = pymysql.connect(
        host=parsed.hostname,
        port=parsed.port or 3306,
        user=parsed.username,
        password=parsed.password,
        database=(parsed.path or "").lstrip("/"),
    )
    try:
        with conn.cursor() as cur:
            if params is not None:
                cur.execute(query, params)
            else:
                cur.execute(query)
            affected_rows = cur.rowcount
        conn.commit()
        return affected_rows
    finally:
        conn.close()


class DbExecRunner(BaseRunner):
    """Execute DML SQL with commit and return the number of affected rows."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "connection": {
                    "type": "string",
                    "description": "Named connection (ConnectionManager) or bare DSN string",
                },
                "query": {
                    "type": "string",
                    "description": "SQL DML query to execute",
                },
                "params": {
                    "type": "array",
                    "description": "Positional query parameters for parametrised execution",
                },
            },
            "required": ["connection", "query"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        conn = config.get("connection")
        if conn is not None and not isinstance(conn, str):
            errors.append("'connection' must be a string")
        query = config.get("query")
        if query is not None and not isinstance(query, str):
            errors.append("'query' must be a string")
        params = config.get("params")
        if params is not None and not isinstance(params, list):
            errors.append("'params' must be a list")
        return errors

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        timeout_str = getattr(step, "timeout", None)
        timeout_seconds = parse_timeout(timeout_str) if timeout_str else config.TIMEOUT_DB

        try:
            return await asyncio.wait_for(
                self._execute_inner(step, context, start),
                timeout=timeout_seconds,
            )
        except asyncio.TimeoutError:
            return {
                "success": False,
                "error": f"Timeout after {timeout_seconds}s",
                "duration": time.monotonic() - start,
            }

    async def _execute_inner(self, step: Any, context: Any, start: float) -> dict:
        self.report_progress(0.0, "starting db_exec")

        step_params = getattr(step, "params", None)
        config_params = step_params if isinstance(step_params, dict) else {}
        query_params = None if isinstance(step_params, dict) else step_params

        connection_ref = config_params.get("connection") or getattr(step, "connection", None) or ""
        query = config_params.get("query") or getattr(step, "query", None) or ""
        params = config_params.get("params", query_params)

        if not connection_ref:
            return {
                "success": False,
                "error": "db_exec step requires 'connection' field",
                "duration": time.monotonic() - start,
            }
        if not query:
            return {
                "success": False,
                "error": "db_exec step requires 'query' field",
                "duration": time.monotonic() - start,
            }

        try:
            driver, dsn = self._resolve_connection(connection_ref, context)
            affected_rows = self._run_exec(driver, dsn, query, params)
        except Exception as exc:
            self.report_progress(0.0, "error: SQL execution failed")
            return {
                "success": False,
                "error": str(exc),
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start
        self.report_progress(100.0, f"{affected_rows} rows affected")
        return {
            "success": True,
            "data": {"affected_rows": affected_rows, "success": True},
            "duration": duration,
        }

    @staticmethod
    def _resolve_connection(connection_ref: str, context: Any) -> tuple[str, str]:
        try:
            from brix.connections import ConnectionManager
            from brix.db import BrixDB

            db = BrixDB()
            manager = ConnectionManager(db)
            conn = manager.get(connection_ref)
            return conn.driver, conn.dsn
        except Exception:
            return _detect_driver(connection_ref), connection_ref

    @staticmethod
    def _run_exec(driver: str, dsn: str, query: str, params: list | tuple | dict | None) -> int:
        if driver == "sqlite":
            return _execute_sqlite(dsn, query, params)
        if driver == "postgresql":
            return _execute_postgresql(dsn, query, params)
        if driver == "duckdb":
            return _execute_duckdb(dsn, query, params)
        if driver == "mysql":
            return _execute_mysql(dsn, query, params)
        raise ValueError(f"Unsupported driver '{driver}'. Supported: sqlite, postgresql, duckdb, mysql.")
