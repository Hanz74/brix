"""DB Query runner — executes parametrised SQL SELECT statements."""
from __future__ import annotations

import asyncio
import time
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout


def _detect_driver(dsn: str) -> str:
    """Infer driver from a bare DSN string (no named connection).

    Supports:
    - ``postgresql://`` / ``postgres://`` → postgresql
    - ``sqlite://``  or a bare file path / ``:memory:`` → sqlite
    - ``mysql://`` / ``mysql+pymysql://`` → mysql

    Falls back to ``sqlite`` when the DSN looks like a plain file path or
    ``:memory:``.
    """
    lower = dsn.lower()
    if lower.startswith(("postgresql://", "postgres://")):
        return "postgresql"
    if lower.startswith("mysql"):
        return "mysql"
    # sqlite:// or bare path / :memory:
    return "sqlite"


def _strip_sqlite_prefix(dsn: str) -> str:
    """Convert ``sqlite:///path`` → ``/path`` for use with stdlib sqlite3.

    SQLite DSN conventions (RFC 3986 file URI):
    - ``sqlite:///abs/path``  → ``/abs/path``  (3 slashes: scheme + empty
      authority + absolute path)
    - ``sqlite:///:memory:``  → ``:memory:``
    - bare path or ``:memory:`` passed through unchanged

    Implementation: strip the 9-char ``sqlite://`` prefix to keep the
    leading ``/`` for absolute paths.
    """
    lower = dsn.lower()
    if lower.startswith("sqlite://"):
        remainder = dsn[9:]  # strip 'sqlite://' (9 chars)
        if not remainder:
            return ":memory:"
        # Handle special token '/:memory:' → ':memory:'
        if remainder.lower() == "/:memory:":
            return ":memory:"
        return remainder
    return dsn  # already a bare path or :memory:


def _execute_sqlite(dsn: str, query: str, params: dict | None) -> list[dict]:
    """Run *query* against a SQLite database and return rows as dicts."""
    import sqlite3

    path = _strip_sqlite_prefix(dsn)
    with sqlite3.connect(path) as conn:
        conn.row_factory = sqlite3.Row
        cur = conn.cursor()
        if params:
            cur.execute(query, params)
        else:
            cur.execute(query)
        rows = cur.fetchall()
    return [dict(row) for row in rows]


def _execute_postgresql(dsn: str, query: str, params: dict | None) -> list[dict]:
    """Run *query* against a PostgreSQL database and return rows as dicts.

    Requires ``psycopg2-binary``.
    """
    import psycopg2  # type: ignore
    import psycopg2.extras  # type: ignore

    conn = psycopg2.connect(dsn)
    try:
        with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
            cur.execute(query, params or None)
            rows = cur.fetchall()
        return [dict(row) for row in rows]
    finally:
        conn.close()


class DbQueryRunner(BaseRunner):
    """Executes a parametrised SQL SELECT and returns rows as a list of dicts.

    Supported databases: SQLite (via stdlib) and PostgreSQL (via psycopg2).

    ``connection`` may be:
    - A named connection registered with the ConnectionManager (resolved via
      CredentialStore / env var).
    - A bare DSN string (``sqlite:///path``, ``:memory:``,
      ``postgresql://user:pw@host/db``).

    ``query`` is rendered as a Jinja2 template so pipeline expressions like
    ``{{ input.table }}`` work.  For *values*, use the ``params`` dict and
    named placeholders — this delegates escaping to the DB driver and avoids
    SQL-injection.

    Result format::

        {
            "rows":      [...],      # list of row dicts
            "row_count": <int>,
            "columns":   [...]       # column names (order from cursor)
        }
    """

    # ------------------------------------------------------------------
    # BaseRunner interface
    # ------------------------------------------------------------------

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
                    "description": "SQL SELECT query; Jinja2 expressions like {{ input.limit }} are supported",
                },
                "params": {
                    "type": "object",
                    "description": "Named query parameters for parametrised queries (SQL-injection-safe)",
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
        return errors

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "list[dict]"

    # ------------------------------------------------------------------
    # Execution
    # ------------------------------------------------------------------

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        # Resolve timeout
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
        self.report_progress(0, "starting db_query")

        # ---- extract config from step --------------------------------
        step_params = getattr(step, "params", None)
        if not isinstance(step_params, dict) or not step_params:
            config_params = getattr(step, "config", None)
            step_params = config_params if isinstance(config_params, dict) else {}

        connection_ref: str = (
            getattr(step, "connection", None)
            or step_params.get("connection")
            or ""
        )
        query_template: str = (
            getattr(step, "query", None)
            or step_params.get("query")
            or ""
        )
        params: dict | None = step_params.get("params")

        if not connection_ref:
            self.report_progress(0.0, "error: missing connection")
            return {
                "success": False,
                "error": "db_query step requires 'connection' field",
                "duration": time.monotonic() - start,
            }
        if not query_template:
            self.report_progress(0.0, "error: missing query")
            return {
                "success": False,
                "error": "db_query step requires 'query' field",
                "duration": time.monotonic() - start,
            }

        # ---- render query via Jinja2 ---------------------------------
        try:
            query = self._render_query(query_template, context)
        except Exception as exc:
            self.report_progress(0.0, "error: query render failed")
            return {
                "success": False,
                "error": f"Jinja2 render error in query: {exc}",
                "duration": time.monotonic() - start,
            }

        # ---- resolve connection --------------------------------------
        try:
            driver, dsn = self._resolve_connection(connection_ref, context)
        except Exception as exc:
            self.report_progress(0.0, "error: connection failed")
            return {
                "success": False,
                "error": f"Connection error: {exc}",
                "duration": time.monotonic() - start,
            }

        # ---- execute SQL ---------------------------------------------
        try:
            rows = self._run_query(driver, dsn, query, params)
        except Exception as exc:
            self.report_progress(0.0, "error: SQL execution failed")
            return {
                "success": False,
                "error": f"SQL error: {exc}",
                "duration": time.monotonic() - start,
            }

        columns: list[str] = list(rows[0].keys()) if rows else []
        data = {
            "rows": rows,
            "row_count": len(rows),
            "columns": columns,
        }

        duration = time.monotonic() - start
        self.report_progress(100, f"{len(rows)} rows returned")
        return {"success": True, "data": data, "duration": duration}

    # ------------------------------------------------------------------
    # Helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _render_query(template_str: str, context: Any) -> str:
        """Render *template_str* with Jinja2 using the pipeline context."""
        from jinja2.sandbox import SandboxedEnvironment

        env = SandboxedEnvironment()
        tmpl = env.from_string(template_str)

        jinja_ctx: dict = {}
        if context is not None and hasattr(context, "to_jinja_context"):
            jinja_ctx = context.to_jinja_context()
        elif context is not None and hasattr(context, "input"):
            jinja_ctx = {"input": context.input}

        return tmpl.render(**jinja_ctx)

    @staticmethod
    def _resolve_connection(connection_ref: str, context: Any) -> tuple[str, str]:
        """Return ``(driver, dsn)`` for *connection_ref*.

        Resolution order:
        1. ConnectionManager (named connection stored in brix.db)
        2. Direct DSN fallback (connection_ref IS the DSN)
        """
        # Try ConnectionManager first (requires a BrixDB instance)
        try:
            from brix.db import BrixDB
            from brix.connections import ConnectionManager, ConnectionNotFoundError

            db = BrixDB()
            manager = ConnectionManager(db)
            conn = manager.get(connection_ref)
            return conn.driver, conn.dsn
        except Exception:
            pass  # Fall through to direct DSN

        # Treat connection_ref as a bare DSN
        driver = _detect_driver(connection_ref)
        return driver, connection_ref

    @staticmethod
    def _run_query(driver: str, dsn: str, query: str, params: dict | None) -> list[dict]:
        """Dispatch to the appropriate DB driver."""
        if driver == "sqlite":
            return _execute_sqlite(dsn, query, params)
        elif driver == "postgresql":
            return _execute_postgresql(dsn, query, params)
        else:
            raise ValueError(
                f"Unsupported driver '{driver}'. Supported: sqlite, postgresql."
            )
