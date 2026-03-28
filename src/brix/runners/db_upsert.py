"""DB Upsert runner — generic INSERT / UPSERT for SQLite and PostgreSQL."""
from __future__ import annotations

import time
from typing import Any


from brix.runners.base import BaseRunner


class DbUpsertRunner(BaseRunner):
    """Generischer INSERT/UPDATE (Upsert) Runner.

    Inserts one or more rows into a table.  When *conflict_key* is provided
    the statement becomes an UPSERT:

    - PostgreSQL: ``ON CONFLICT (...) DO UPDATE SET col = EXCLUDED.col``
    - SQLite: ``INSERT OR REPLACE INTO ...``

    *data* may come either from the step config/params directly or from the
    previous step's output stored in the pipeline context.

    Result dict:
        ``{"inserted": N, "updated": M, "total": N+M}``

    All SQL is built with parameter binding — no string interpolation of
    user-supplied values.
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
                    "description": "Named connection registered via ConnectionManager",
                },
                "table": {
                    "type": "string",
                    "description": "Target table name",
                },
                "data": {
                    "type": ["object", "array"],
                    "description": "Dict or list of dicts to insert",
                },
                "conflict_key": {
                    "type": ["string", "array"],
                    "description": "Column(s) used for ON CONFLICT clause",
                },
                "set_columns": {
                    "type": "array",
                    "description": "Columns to update on conflict (defaults to all non-key columns)",
                },
            },
            "required": ["connection", "table"],
        }

    def input_type(self) -> str:
        return "dict"

    def output_type(self) -> str:
        return "dict"

    # ------------------------------------------------------------------
    # Execute
    # ------------------------------------------------------------------

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        self.report_progress(0.0, "starting")

        params = getattr(step, "params", {}) or {}
        conn_name: str = params.get("connection") or getattr(step, "connection", None)
        table: str = params.get("table") or getattr(step, "table", None)

        if not conn_name:
            return {
                "success": False,
                "error": "db_upsert step requires 'connection'",
                "duration": time.monotonic() - start,
            }
        if not table:
            return {
                "success": False,
                "error": "db_upsert step requires 'table'",
                "duration": time.monotonic() - start,
            }

        # Resolve data: config params → pipeline context (previous step output)
        data = params.get("data")
        if data is None:
            data = getattr(step, "data", None)
        if data is None and context is not None:
            # Try the last step's output stored in context
            step_outputs = getattr(context, "step_outputs", {})
            if step_outputs:
                last_output = list(step_outputs.values())[-1]
                if isinstance(last_output, dict) and "data" in last_output:
                    data = last_output["data"]
                elif isinstance(last_output, (dict, list)):
                    data = last_output

        # Normalise to list
        if data is None:
            rows: list[dict] = []
        elif isinstance(data, dict):
            rows = [data]
        elif isinstance(data, list):
            rows = data
        else:
            return {
                "success": False,
                "error": f"db_upsert 'data' must be a dict or list of dicts, got {type(data).__name__}",
                "duration": time.monotonic() - start,
            }

        # Empty data — return early with 0 counts
        if not rows:
            self.report_progress(100.0, "done", done=0, total=0)
            return {
                "success": True,
                "data": {"inserted": 0, "updated": 0, "total": 0},
                "duration": time.monotonic() - start,
            }

        # Validate all rows are dicts
        for i, row in enumerate(rows):
            if not isinstance(row, dict):
                return {
                    "success": False,
                    "error": f"db_upsert: row {i} is not a dict (got {type(row).__name__})",
                    "duration": time.monotonic() - start,
                }

        # conflict_key — normalise to list
        conflict_key_raw = params.get("conflict_key") or getattr(step, "conflict_key", None)
        if isinstance(conflict_key_raw, str):
            conflict_keys: list[str] = [conflict_key_raw]
        elif isinstance(conflict_key_raw, list):
            conflict_keys = conflict_key_raw
        else:
            conflict_keys = []

        set_columns: list[str] | None = params.get("set_columns") or getattr(step, "set_columns", None)

        # Resolve connection
        try:
            connection = self._resolve_connection(conn_name)
        except Exception as exc:
            return {
                "success": False,
                "error": f"db_upsert: could not resolve connection '{conn_name}': {exc}",
                "duration": time.monotonic() - start,
            }

        driver = connection.driver
        dsn = connection.dsn

        try:
            result = _execute_upsert(
                driver=driver,
                dsn=dsn,
                table=table,
                rows=rows,
                conflict_keys=conflict_keys,
                set_columns=set_columns,
            )
        except Exception as exc:
            return {
                "success": False,
                "error": str(exc),
                "duration": time.monotonic() - start,
            }

        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=result["total"], total=result["total"])
        return {
            "success": True,
            "data": result,
            "duration": duration,
        }

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _resolve_connection(self, name: str) -> Any:
        """Resolve a named connection via ConnectionManager.

        Falls back to treating *name* as a literal DSN with sqlite driver for
        testing convenience (when the name starts with ``':memory:'`` or ends
        with ``'.db'``).
        """
        from brix.db import BrixDB
        from brix.connections import ConnectionManager, ConnectionNotFoundError, Connection

        try:
            db = BrixDB()
            mgr = ConnectionManager(db)
            return mgr.get(name)
        except ConnectionNotFoundError:
            # Allow ':memory:' and file paths as direct DSNs for tests
            if name == ":memory:" or name.endswith(".db"):
                return Connection(name=name, driver="sqlite", dsn=name)
            raise


# ---------------------------------------------------------------------------
# SQL generation helpers (module-level, pure functions — easy to unit-test)
# ---------------------------------------------------------------------------


def _build_insert_sql(
    driver: str,
    table: str,
    columns: list[str],
    conflict_keys: list[str],
    set_columns: list[str] | None,
) -> str:
    """Build a parameterised INSERT (or UPSERT) SQL statement.

    Args:
        driver:        One of 'sqlite', 'postgresql', 'mysql', 'duckdb'.
        table:         Target table name (not user-interpolated into values).
        columns:       Column names in insertion order.
        conflict_keys: Columns that form the conflict target.  Empty = plain INSERT.
        set_columns:   Columns to update on conflict.  Defaults to all non-key columns.

    Returns:
        A parameterised SQL string.  Placeholders are ``?`` for SQLite and
        ``%s`` for PostgreSQL / MySQL / DuckDB.
    """
    placeholder = "?" if driver == "sqlite" else "%s"
    cols_sql = ", ".join(f'"{c}"' for c in columns)
    vals_sql = ", ".join(placeholder for _ in columns)
    base = f'INSERT INTO "{table}" ({cols_sql}) VALUES ({vals_sql})'

    if not conflict_keys:
        return base

    if driver == "sqlite":
        # SQLite: INSERT OR REPLACE (full-row replace on conflict)
        return f'INSERT OR REPLACE INTO "{table}" ({cols_sql}) VALUES ({vals_sql})'

    # PostgreSQL / DuckDB / MySQL: ON CONFLICT ... DO UPDATE SET
    conflict_cols_sql = ", ".join(f'"{k}"' for k in conflict_keys)
    update_cols = set_columns if set_columns else [c for c in columns if c not in conflict_keys]
    if not update_cols:
        # All columns are conflict keys — nothing to update, use DO NOTHING
        return f"{base} ON CONFLICT ({conflict_cols_sql}) DO NOTHING"

    set_clauses = ", ".join(f'"{c}" = EXCLUDED."{c}"' for c in update_cols)
    return f"{base} ON CONFLICT ({conflict_cols_sql}) DO UPDATE SET {set_clauses}"


def _execute_upsert(
    driver: str,
    dsn: str,
    table: str,
    rows: list[dict],
    conflict_keys: list[str],
    set_columns: list[str] | None,
) -> dict:
    """Execute INSERT/UPSERT and return counts dict.

    All rows are inserted in a single transaction.  The function counts
    inserted vs updated rows using rowcount heuristics:

    - SQLite ``INSERT OR REPLACE``: rowcount=1 per successful row.  We
      distinguish insert vs update by checking whether rowcount after the
      statement equals 1 (insert) or 2 (delete + insert for replace).
      In practice SQLite does not expose this easily, so we report
      ``inserted=total`` for plain INSERT and ``inserted=N, updated=0`` for
      INSERT OR REPLACE (conservative — avoids complex pre-queries).
    - PostgreSQL: ``ON CONFLICT DO UPDATE`` → rowcount=1 for both insert and
      update.  We track updated count by checking ``xmax`` is non-zero, but
      that requires a SELECT which is expensive.  Instead we use a simpler
      approach: after execute, ``cursor.rowcount`` is 1 for both; we report
      ``inserted=N, updated=0`` unless conflict_key is set in which case
      ``inserted`` and ``updated`` are both approximations.  For production
      accuracy users should rely on ``total``.
    """
    has_conflict = bool(conflict_keys)
    total = len(rows)

    if driver == "sqlite":
        return _execute_sqlite(dsn, table, rows, conflict_keys, set_columns, has_conflict, total)
    elif driver == "postgresql":
        return _execute_postgres(dsn, table, rows, conflict_keys, set_columns, has_conflict, total)
    elif driver in ("duckdb", "mysql"):
        return _execute_generic_dbapi(driver, dsn, table, rows, conflict_keys, set_columns, has_conflict, total)
    else:
        raise ValueError(f"db_upsert: unsupported driver '{driver}'")


def _execute_sqlite(
    dsn: str,
    table: str,
    rows: list[dict],
    conflict_keys: list[str],
    set_columns: list[str] | None,
    has_conflict: bool,
    total: int,
) -> dict:
    import sqlite3

    conn = sqlite3.connect(dsn)
    try:
        # Verify table exists
        cursor = conn.execute(
            "SELECT name FROM sqlite_master WHERE type='table' AND name=?", (table,)
        )
        if cursor.fetchone() is None:
            raise RuntimeError(f"Table '{table}' does not exist")

        with conn:
            for row in rows:
                columns = list(row.keys())
                sql = _build_insert_sql("sqlite", table, columns, conflict_keys, set_columns)
                values = [row[c] for c in columns]
                conn.execute(sql, values)
    finally:
        conn.close()

    # SQLite doesn't distinguish insert vs replace counts easily
    inserted = total
    updated = 0
    return {"inserted": inserted, "updated": updated, "total": total}


def _execute_postgres(
    dsn: str,
    table: str,
    rows: list[dict],
    conflict_keys: list[str],
    set_columns: list[str] | None,
    has_conflict: bool,
    total: int,
) -> dict:
    try:
        import psycopg2  # type: ignore
    except ImportError as exc:
        raise ImportError(
            f"psycopg2 is required for PostgreSQL connections. Install psycopg2-binary. ({exc})"
        ) from exc

    conn = psycopg2.connect(dsn)
    try:
        with conn:
            with conn.cursor() as cur:
                for row in rows:
                    columns = list(row.keys())
                    sql = _build_insert_sql("postgresql", table, columns, conflict_keys, set_columns)
                    values = [row[c] for c in columns]
                    cur.execute(sql, values)
    finally:
        conn.close()

    inserted = total
    updated = 0
    return {"inserted": inserted, "updated": updated, "total": total}


def _execute_generic_dbapi(
    driver: str,
    dsn: str,
    table: str,
    rows: list[dict],
    conflict_keys: list[str],
    set_columns: list[str] | None,
    has_conflict: bool,
    total: int,
) -> dict:
    """Generic DBAPI2 path for duckdb / mysql."""
    if driver == "duckdb":
        try:
            import duckdb  # type: ignore
        except ImportError as exc:
            raise ImportError(f"duckdb package required. ({exc})") from exc
        conn = duckdb.connect(dsn)
    elif driver == "mysql":
        try:
            import pymysql  # type: ignore
        except ImportError as exc:
            raise ImportError(f"pymysql package required. ({exc})") from exc
        import urllib.parse
        r = urllib.parse.urlparse(dsn)
        conn = pymysql.connect(
            host=r.hostname,
            port=r.port or 3306,
            user=r.username,
            password=r.password,
            database=(r.path or "").lstrip("/"),
        )
    else:
        raise ValueError(f"Unsupported driver: {driver}")

    try:
        cur = conn.cursor()
        for row in rows:
            columns = list(row.keys())
            sql = _build_insert_sql(driver, table, columns, conflict_keys, set_columns)
            values = [row[c] for c in columns]
            cur.execute(sql, values)
        conn.commit()
    finally:
        conn.close()

    return {"inserted": total, "updated": 0, "total": total}
