"""Document persistence runners for reusable extraction-result mutations."""
from __future__ import annotations

import asyncio
import json
import re
import time
import urllib.parse
from typing import Any

from brix.config import config
from brix.runners.base import BaseRunner
from brix.runners.cli import parse_timeout
from brix.runners.db_exec import DbExecRunner
from brix.runners.db_query import _strip_sqlite_prefix
from brix.serialization import sanitize_for_json

_IDENTIFIER_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")


def _ensure_identifier(value: str, field_name: str) -> str:
    candidate = (value or "").strip()
    if not candidate:
        raise ValueError(f"'{field_name}' is required")
    if not _IDENTIFIER_RE.match(candidate):
        raise ValueError(f"'{field_name}' must be a safe SQL identifier")
    return candidate


def _normalize_extraction_result(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return {}
        parsed = json.loads(stripped)
        if isinstance(parsed, dict):
            return parsed
    raise ValueError("'extraction_result' must be an object or JSON object string")


def _decode_specialists(value: Any) -> list[str]:
    if value is None:
        return []
    if isinstance(value, list):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, tuple):
        return [str(item) for item in value if str(item).strip()]
    if isinstance(value, str):
        stripped = value.strip()
        if not stripped:
            return []
        try:
            parsed = json.loads(stripped)
        except Exception:
            parsed = None
        if isinstance(parsed, list):
            return [str(item) for item in parsed if str(item).strip()]
        if stripped.startswith("{") and stripped.endswith("}"):
            inner = stripped[1:-1].strip()
            if not inner:
                return []
            return [part.strip().strip('"') for part in inner.split(",") if part.strip()]
        return [part.strip() for part in stripped.split(",") if part.strip()]
    return []


def _json_or_none(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, dict) and not value:
        return None
    if value == "":
        return None
    return json.dumps(value)


def _extract_doc_type(extraction_result: dict[str, Any]) -> str:
    for key in ("document_type", "doc_type"):
        value = extraction_result.get(key)
        if isinstance(value, str) and value.strip():
            return value.strip()
    normalized = extraction_result.get("normalized")
    if isinstance(normalized, dict):
        value = normalized.get("document_type") or normalized.get("doc_type")
        if isinstance(value, str) and value.strip():
            return value.strip()
    return ""


def _db_placeholder(driver: str) -> str:
    return "?" if driver in {"sqlite", "duckdb"} else "%s"


def _connect(driver: str, dsn: str):
    if driver == "sqlite":
        import sqlite3

        conn = sqlite3.connect(_strip_sqlite_prefix(dsn))
        conn.row_factory = sqlite3.Row
        return conn
    if driver == "duckdb":
        import duckdb  # type: ignore

        return duckdb.connect(dsn)
    if driver == "postgresql":
        import psycopg2  # type: ignore

        return psycopg2.connect(dsn)
    if driver == "mysql":
        import pymysql  # type: ignore

        parsed = urllib.parse.urlparse(dsn)
        return pymysql.connect(
            host=parsed.hostname,
            port=parsed.port or 3306,
            user=parsed.username,
            password=parsed.password,
            database=(parsed.path or "").lstrip("/"),
        )
    raise ValueError(f"Unsupported driver '{driver}'. Supported: sqlite, postgresql, duckdb, mysql.")


def _fetch_specialists(conn: Any, driver: str, table: str, id_field: str, specialists_field: str, document_id: Any) -> list[str] | None:
    placeholder = _db_placeholder(driver)
    query = f"SELECT {specialists_field} FROM {table} WHERE {id_field} = {placeholder}"
    cursor = conn.cursor()
    try:
        cursor.execute(query, (document_id,))
        row = cursor.fetchone()
    finally:
        cursor.close()
    if row is None:
        return None
    raw = row[0] if isinstance(row, tuple) else row[specialists_field]
    return _decode_specialists(raw)


def _encode_specialists(driver: str, specialists: list[str]) -> Any:
    if driver == "postgresql":
        return specialists
    return json.dumps(specialists)


def _execute_update(
    conn: Any,
    driver: str,
    table: str,
    id_field: str,
    document_id: Any,
    assignments: list[tuple[str, Any]],
    json_columns: set[str] | None = None,
) -> int:
    if not assignments:
        return 0
    placeholder = _db_placeholder(driver)
    json_columns = json_columns or set()
    set_parts: list[str] = []
    params: list[Any] = []
    for column, value in assignments:
        if driver == "postgresql" and column in json_columns and isinstance(value, str):
            set_parts.append(f"{column} = CAST({placeholder} AS JSONB)")
        else:
            set_parts.append(f"{column} = {placeholder}")
        params.append(value)
    params.append(document_id)
    query = f"UPDATE {table} SET {', '.join(set_parts)} WHERE {id_field} = {placeholder}"
    cursor = conn.cursor()
    try:
        cursor.execute(query, tuple(params))
        affected = cursor.rowcount
    finally:
        cursor.close()
    conn.commit()
    return affected


class _BaseDocumentRunner(BaseRunner):
    def _timeout_seconds(self, step: Any) -> float:
        timeout_str = getattr(step, "timeout", None)
        return parse_timeout(timeout_str) if timeout_str else config.TIMEOUT_DB

    def _config(self, step: Any) -> dict[str, Any]:
        config_dict = getattr(step, "config", None)
        return config_dict if isinstance(config_dict, dict) else {}

    def _resolve_target(self, step: Any) -> tuple[str, str, str, Any]:
        cfg = self._config(step)
        connection = str(cfg.get("connection") or "").strip()
        if not connection:
            raise ValueError("'connection' is required")
        document_id = cfg.get("document_id")
        if document_id in (None, ""):
            raise ValueError("'document_id' is required")
        table = _ensure_identifier(str(cfg.get("table") or "documents"), "table")
        _ensure_identifier(str(cfg.get("id_field") or "id"), "id_field")
        driver, dsn = DbExecRunner._resolve_connection(connection, context=None)
        return driver, dsn, table, document_id

    def _specialists_field(self, step: Any) -> str:
        return _ensure_identifier(str(self._config(step).get("specialists_field") or "extraction_specialists"), "specialists_field")


class DocumentPersistExtractionResultRunner(_BaseDocumentRunner):
    """Persist extraction result metadata to a standard documents table."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "connection": {"type": "string", "description": "Named connection or DSN"},
                "document_id": {"type": "string", "description": "Primary-key value of the document row"},
                "extraction_result": {"type": "object", "description": "Structured extraction result payload"},
                "content_hash": {"type": "string", "description": "Optional content hash to persist"},
                "file_path": {"type": "string", "description": "Optional persisted local file path"},
                "specialist_name": {"type": "string", "description": "Optional specialist marker to append"},
                "table": {"type": "string", "description": "Document table name", "default": "documents"},
                "id_field": {"type": "string", "description": "Primary-key column name", "default": "id"},
                "raw_field": {"type": "string", "description": "Structured extraction payload column", "default": "raw_structured"},
                "doc_type_field": {"type": "string", "description": "Document type column", "default": "doc_type"},
                "content_hash_field": {"type": "string", "description": "Content-hash column", "default": "content_hash"},
                "file_path_field": {"type": "string", "description": "File-path column", "default": "file_path"},
                "specialists_field": {"type": "string", "description": "Specialist-state column", "default": "extraction_specialists"},
            },
            "required": ["connection", "document_id", "extraction_result"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        try:
            return await asyncio.wait_for(self._execute_inner(step), timeout=self._timeout_seconds(step))
        except asyncio.TimeoutError:
            return {"success": False, "error": "Timeout during document persistence", "duration": time.monotonic() - start}

    async def _execute_inner(self, step: Any) -> dict:
        start = time.monotonic()
        cfg = self._config(step)
        driver, dsn, table, document_id = self._resolve_target(step)
        raw_field = _ensure_identifier(str(cfg.get("raw_field") or "raw_structured"), "raw_field")
        doc_type_field = _ensure_identifier(str(cfg.get("doc_type_field") or "doc_type"), "doc_type_field")
        content_hash_field = _ensure_identifier(str(cfg.get("content_hash_field") or "content_hash"), "content_hash_field")
        file_path_field = _ensure_identifier(str(cfg.get("file_path_field") or "file_path"), "file_path_field")
        specialists_field = self._specialists_field(step)
        extraction_result = _normalize_extraction_result(cfg.get("extraction_result"))
        specialist_name = str(cfg.get("specialist_name") or "").strip()
        content_hash = str(cfg.get("content_hash") or "").strip()
        file_path = str(cfg.get("file_path") or "").strip()

        assignments: list[tuple[str, Any]] = []
        applied_fields: list[str] = []
        raw_json = _json_or_none(extraction_result)
        if raw_json is not None:
            assignments.append((raw_field, raw_json))
            applied_fields.append(raw_field)
        doc_type = _extract_doc_type(extraction_result)
        if doc_type:
            assignments.append((doc_type_field, doc_type))
            applied_fields.append(doc_type_field)
        if content_hash:
            assignments.append((content_hash_field, content_hash))
            applied_fields.append(content_hash_field)
        if file_path:
            assignments.append((file_path_field, file_path))
            applied_fields.append(file_path_field)

        self.report_progress(10.0, "loading current specialist state")
        conn = _connect(driver, dsn)
        try:
            current_specialists = _fetch_specialists(conn, driver, table, _ensure_identifier(str(cfg.get("id_field") or "id"), "id_field"), specialists_field, document_id)
            if current_specialists is None:
                conn.rollback()
                return {
                    "success": True,
                    "data": sanitize_for_json({"affected_rows": 0, "success": True, "applied_fields": [], "document_id": document_id}),
                    "duration": time.monotonic() - start,
                }
            if specialist_name and specialist_name not in current_specialists:
                current_specialists.append(specialist_name)
                assignments.append((specialists_field, _encode_specialists(driver, current_specialists)))
                applied_fields.append(specialists_field)

            self.report_progress(70.0, "persisting extraction result")
            affected_rows = _execute_update(
                conn,
                driver,
                table,
                _ensure_identifier(str(cfg.get("id_field") or "id"), "id_field"),
                document_id,
                assignments,
                json_columns={raw_field},
            )
        finally:
            conn.close()

        self.report_progress(100.0, f"{affected_rows} document rows updated")
        return {
            "success": True,
            "data": sanitize_for_json(
                {
                    "affected_rows": affected_rows,
                    "success": True,
                    "applied_fields": applied_fields,
                    "document_id": document_id,
                }
            ),
            "duration": time.monotonic() - start,
        }


class DocumentMarkSpecialistProcessedRunner(_BaseDocumentRunner):
    """Append a specialist marker to a document row exactly once."""

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "connection": {"type": "string", "description": "Named connection or DSN"},
                "document_id": {"type": "string", "description": "Primary-key value of the document row"},
                "specialist_name": {"type": "string", "description": "Specialist marker to append"},
                "table": {"type": "string", "description": "Document table name", "default": "documents"},
                "id_field": {"type": "string", "description": "Primary-key column name", "default": "id"},
                "specialists_field": {"type": "string", "description": "Specialist-state column", "default": "extraction_specialists"},
            },
            "required": ["connection", "document_id", "specialist_name"],
        }

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()
        try:
            return await asyncio.wait_for(self._execute_inner(step), timeout=self._timeout_seconds(step))
        except asyncio.TimeoutError:
            return {"success": False, "error": "Timeout during specialist marker update", "duration": time.monotonic() - start}

    async def _execute_inner(self, step: Any) -> dict:
        start = time.monotonic()
        cfg = self._config(step)
        driver, dsn, table, document_id = self._resolve_target(step)
        id_field = _ensure_identifier(str(cfg.get("id_field") or "id"), "id_field")
        specialists_field = self._specialists_field(step)
        specialist_name = str(cfg.get("specialist_name") or "").strip()
        if not specialist_name:
            raise ValueError("'specialist_name' is required")

        self.report_progress(10.0, "loading current specialist state")
        conn = _connect(driver, dsn)
        try:
            current_specialists = _fetch_specialists(conn, driver, table, id_field, specialists_field, document_id)
            if current_specialists is None:
                conn.rollback()
                return {
                    "success": True,
                    "data": sanitize_for_json({"affected_rows": 0, "success": True, "document_id": document_id, "specialist_added": False}),
                    "duration": time.monotonic() - start,
                }
            specialist_added = specialist_name not in current_specialists
            if specialist_added:
                current_specialists.append(specialist_name)
            self.report_progress(70.0, "persisting specialist marker")
            affected_rows = _execute_update(
                conn,
                driver,
                table,
                id_field,
                document_id,
                [(specialists_field, _encode_specialists(driver, current_specialists))] if specialist_added else [],
            )
        finally:
            conn.close()

        self.report_progress(100.0, f"{affected_rows} document rows updated")
        return {
            "success": True,
            "data": sanitize_for_json(
                {
                    "affected_rows": affected_rows,
                    "success": True,
                    "document_id": document_id,
                    "specialist_added": specialist_added,
                }
            ),
            "duration": time.monotonic() - start,
        }
