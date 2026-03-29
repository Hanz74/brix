"""Connection Manager — Named DB connections for db.query and db.upsert runners.

Connections are stored in the main brix.db (connections table).
DSNs are encrypted via CredentialStore — never stored in plaintext.

Resolution priority:
  1. Connection name in DB → DSN from Credential Store (encrypted)
  2. Fallback: ENV-Variable (e.g. BUDDY_DB_URL)
  3. No match → ConnectionNotFoundError
"""
from __future__ import annotations

import logging
import os
import sqlite3
import uuid
from contextlib import contextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Generator, Optional

from brix.credential_store import CredentialStore, CredentialNotFoundError

logger = logging.getLogger(__name__)

DEFAULT_DRIVER = "postgresql"
SUPPORTED_DRIVERS = ("postgresql", "sqlite", "mysql", "duckdb")


class ConnectionNotFoundError(KeyError):
    """Raised when a named connection cannot be resolved."""
    pass


class Connection:
    """A resolved connection with its DSN and metadata."""

    def __init__(
        self,
        name: str,
        driver: str,
        dsn: str,
        description: str = "",
        env_var: Optional[str] = None,
    ):
        self.name = name
        self.driver = driver
        self.dsn = dsn
        self.description = description
        self.env_var = env_var

    def test(self) -> dict:
        """Attempt to connect and ping. Returns dict with success and message."""
        try:
            if self.driver == "sqlite":
                return self._test_sqlite()
            elif self.driver in ("postgresql", "mysql", "duckdb"):
                return self._test_generic_dbapi()
            else:
                return {"success": False, "error": f"Unsupported driver: {self.driver}"}
        except Exception as exc:
            return {"success": False, "error": str(exc)}

    def _test_sqlite(self) -> dict:
        import sqlite3 as _sqlite3
        # DSN for sqlite is a file path or :memory:
        path = self.dsn
        try:
            conn = _sqlite3.connect(path, timeout=5)
            conn.execute("SELECT 1")
            conn.close()
            return {"success": True, "driver": self.driver, "message": f"Connected to SQLite: {path}"}
        except Exception as exc:
            return {"success": False, "driver": self.driver, "error": str(exc)}

    def _test_generic_dbapi(self) -> dict:
        """Generic test via appropriate DB driver library."""
        driver = self.driver
        dsn = self.dsn
        try:
            if driver == "postgresql":
                import psycopg2  # type: ignore
                conn = psycopg2.connect(dsn, connect_timeout=5)
                cur = conn.cursor()
                cur.execute("SELECT 1")
                cur.close()
                conn.close()
                return {"success": True, "driver": driver, "message": "PostgreSQL ping successful"}
            elif driver == "mysql":
                import pymysql  # type: ignore
                # pymysql accepts DSN as URL — parse it
                import urllib.parse
                r = urllib.parse.urlparse(dsn)
                conn = pymysql.connect(
                    host=r.hostname,
                    port=r.port or 3306,
                    user=r.username,
                    password=r.password,
                    database=(r.path or "").lstrip("/"),
                    connect_timeout=5,
                )
                conn.ping()
                conn.close()
                return {"success": True, "driver": driver, "message": "MySQL ping successful"}
            elif driver == "duckdb":
                import duckdb  # type: ignore
                conn = duckdb.connect(dsn)
                conn.execute("SELECT 1")
                conn.close()
                return {"success": True, "driver": driver, "message": "DuckDB ping successful"}
            else:
                return {"success": False, "error": f"No test implementation for driver: {driver}"}
        except ImportError as exc:
            return {
                "success": False,
                "driver": driver,
                "error": f"Driver package not installed: {exc}. "
                         f"Install the appropriate package (e.g. psycopg2-binary for postgresql).",
            }
        except Exception as exc:
            return {"success": False, "driver": driver, "error": str(exc)}

    def __repr__(self) -> str:
        return f"Connection(name={self.name!r}, driver={self.driver!r})"


class ConnectionManager:
    """Named DB-Connections for db.query and db.upsert Runner.

    Stores connection metadata in brix.db (connections table).
    DSNs are encrypted and referenced via CredentialStore.

    Resolution order:
      1. DB entry → CredentialStore (encrypted DSN)
      2. ENV-Variable fallback (env_var field)
      3. ConnectionNotFoundError
    """

    def __init__(self, db: "BrixDB"):  # type: ignore[name-defined]
        self._db = db
        self._cred_store = CredentialStore()

    def _now_iso(self) -> str:
        return datetime.now(timezone.utc).isoformat()

    # ------------------------------------------------------------------
    # Public CRUD
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        dsn: str,
        driver: str = DEFAULT_DRIVER,
        description: str = "",
        env_var: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Register a named connection. DSN is encrypted via CredentialStore.

        Returns metadata dict (no DSN).
        Raises ValueError if driver is unsupported.
        Raises sqlite3.IntegrityError if name already exists.
        """
        name = name.strip()
        if not name:
            raise ValueError("Connection name must not be empty")
        if driver not in SUPPORTED_DRIVERS:
            raise ValueError(
                f"Unsupported driver '{driver}'. "
                f"Supported: {', '.join(SUPPORTED_DRIVERS)}"
            )
        if not dsn:
            raise ValueError("DSN must not be empty")

        conn_id = str(uuid.uuid4())
        now = self._now_iso()

        # Store DSN encrypted in CredentialStore
        cred_name = f"connection_dsn_{name}"
        # If a cred with that name already exists (e.g. re-registration), update it
        try:
            existing_cred = self._cred_store.get(cred_name)
            self._cred_store.update(cred_name, value=dsn)
            dsn_cred_id = existing_cred["id"]
        except CredentialNotFoundError:
            dsn_cred_id = self._cred_store.add(cred_name, "api-key", dsn)

        with self._db._connect() as conn:
            # T-BRIX-ORG-01: include org fields if columns exist
            cols = ["id", "name", "driver", "dsn_credential_id", "env_var", "description", "created_at", "updated_at"]
            vals = [conn_id, name, driver, dsn_cred_id, env_var, description, now, now]

            if project is not None and self._db._column_exists(conn, "connections", "project"):
                cols.append("project")
                vals.append(project)
            if tags is not None and self._db._column_exists(conn, "connections", "tags"):
                import json as _json
                cols.append("tags")
                vals.append(_json.dumps(tags))
            if group_name is not None and self._db._column_exists(conn, "connections", "group_name"):
                cols.append("group_name")
                vals.append(group_name)

            placeholders = ",".join("?" * len(cols))
            conn.execute(
                f"INSERT INTO connections ({','.join(cols)}) VALUES ({placeholders})",
                vals,
            )

        meta = self._row_to_meta(
            conn_id, name, driver, dsn_cred_id, env_var, description, now, now
        )
        if project is not None:
            meta["project"] = project
        if tags is not None:
            meta["tags"] = tags
        if group_name is not None:
            meta["group"] = group_name
        return meta

    def get(self, name: str) -> Connection:
        """Resolve a connection by name. Returns a Connection with decrypted DSN.

        Resolution:
          1. DB entry → CredentialStore (encrypted DSN)
          2. ENV-Variable fallback
          3. ConnectionNotFoundError
        """
        row = self._find_row(name)
        if row is None:
            raise ConnectionNotFoundError(f"Connection not found: '{name}'")

        # Try CredentialStore first
        if row["dsn_credential_id"]:
            try:
                dsn = self._cred_store.resolve(row["dsn_credential_id"])
                return Connection(
                    name=row["name"],
                    driver=row["driver"],
                    dsn=dsn,
                    description=row["description"] or "",
                    env_var=row["env_var"],
                )
            except CredentialNotFoundError:
                pass  # Fall through to ENV-Variable

        # Fallback: ENV-Variable
        if row["env_var"]:
            dsn = os.environ.get(row["env_var"], "")
            if dsn:
                return Connection(
                    name=row["name"],
                    driver=row["driver"],
                    dsn=dsn,
                    description=row["description"] or "",
                    env_var=row["env_var"],
                )

        raise ConnectionNotFoundError(
            f"Connection '{name}' found in DB but DSN could not be resolved. "
            f"Check that credential '{row['dsn_credential_id']}' exists or set env var '{row['env_var']}'."
        )

    def list(self) -> list[dict]:
        """List all registered connections (metadata only — no DSN)."""
        import json as _json
        with self._db._connect() as conn:
            conn.row_factory = sqlite3.Row
            rows = conn.execute(
                "SELECT * FROM connections ORDER BY name"
            ).fetchall()
        result = []
        for r in rows:
            d = self._row_to_meta(
                r["id"], r["name"], r["driver"], r["dsn_credential_id"],
                r["env_var"], r["description"], r["created_at"], r["updated_at"]
            )
            # T-BRIX-ORG-01: enrich with org fields
            rd = dict(r)
            d["project"] = rd.get("project", "") or ""
            raw_tags = rd.get("tags", "[]")
            if isinstance(raw_tags, str):
                try:
                    d["tags"] = _json.loads(raw_tags)
                except (ValueError, TypeError):
                    d["tags"] = []
            else:
                d["tags"] = raw_tags if isinstance(raw_tags, list) else []
            d["group"] = rd.get("group_name", "") or ""
            result.append(d)
        return result

    def delete(self, name: str) -> bool:
        """Delete a connection and its associated credential. Returns True if deleted."""
        row = self._find_row(name)
        if row is None:
            return False

        # Delete associated credential
        if row["dsn_credential_id"]:
            self._cred_store.delete(row["dsn_credential_id"])

        with self._db._connect() as conn:
            conn.execute("DELETE FROM connections WHERE id = ?", (row["id"],))
        return True

    def test(self, name: str) -> dict:
        """Test a connection by name. Returns success/error dict."""
        try:
            conn = self.get(name)
        except ConnectionNotFoundError as exc:
            return {"success": False, "name": name, "error": str(exc)}
        result = conn.test()
        result["name"] = name
        return result

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _find_row(self, name: str) -> Optional[sqlite3.Row]:
        """Find a connection row by name."""
        with self._db._connect() as conn:
            conn.row_factory = sqlite3.Row
            row = conn.execute(
                "SELECT * FROM connections WHERE name = ?", (name,)
            ).fetchone()
        return row

    @staticmethod
    def _row_to_meta(
        conn_id: str,
        name: str,
        driver: str,
        dsn_credential_id: Optional[str],
        env_var: Optional[str],
        description: Optional[str],
        created_at: str,
        updated_at: Optional[str],
    ) -> dict:
        """Convert row data to metadata dict (no DSN)."""
        return {
            "id": conn_id,
            "name": name,
            "driver": driver,
            "dsn_credential_id": dsn_credential_id,
            "env_var": env_var,
            "description": description or "",
            "created_at": created_at,
            "updated_at": updated_at,
        }
