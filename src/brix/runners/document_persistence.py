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


def _is_nonempty_string(value: Any) -> bool:
    return isinstance(value, str) and bool(value.strip())


def _is_number(value: Any) -> bool:
    return isinstance(value, (int, float)) and not isinstance(value, bool)


def validate_canonical_extraction_result(extraction_result: dict[str, Any]) -> list[dict[str, str]]:
    """Validate the canonical Daigestr contract expected by persistence bricks.

    The authoritative contract is:
    - raw.meta for technical/document metadata
    - raw.extracted for extracted business fields
    - raw.normalized for normalized business fields
    """

    issues: list[dict[str, str]] = []
    raw = extraction_result.get("raw")
    if not isinstance(raw, dict):
        return [{
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw",
            "message": "Missing canonical 'raw' object in extraction_result.",
        }]

    meta = raw.get("meta")
    if not isinstance(meta, dict):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.meta",
            "message": "Missing canonical 'raw.meta' object.",
        })
        meta = {}

    extracted = raw.get("extracted")
    if not isinstance(extracted, dict):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.extracted",
            "message": "Missing canonical 'raw.extracted' object.",
        })

    normalized = raw.get("normalized")
    if not isinstance(normalized, dict):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.normalized",
            "message": "Missing canonical 'raw.normalized' object.",
        })

    if not _is_nonempty_string(meta.get("document_type")):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.meta.document_type",
            "message": "Missing canonical document type in raw.meta.document_type.",
        })

    if not _is_nonempty_string(meta.get("template_used")):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.meta.template_used",
            "message": "Missing canonical template in raw.meta.template_used.",
        })

    if not _is_number(meta.get("quality_score")):
        issues.append({
            "code": "EXTRACTION_RESULT_CANONICAL_CONTRACT",
            "field": "raw.meta.quality_score",
            "message": "Missing canonical quality score in raw.meta.quality_score.",
        })

    return issues


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
    raw = extraction_result.get("raw")
    if isinstance(raw, dict):
        meta = raw.get("meta")
        if isinstance(meta, dict):
            value = meta.get("document_type")
            if isinstance(value, str) and value.strip():
                return value.strip()
    return ""


def _normalize_statement_numbers(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str):
        return [part.strip() for part in value.split(",") if part.strip()]
    return []


def _statement_bundle_summary(extraction_result: dict[str, Any]) -> dict[str, Any]:
    raw = extraction_result.get("raw")
    normalized = raw.get("normalized") if isinstance(raw, dict) else None
    if not isinstance(normalized, dict):
        normalized = extraction_result.get("normalized")
    if not isinstance(normalized, dict):
        return {
            "is_bundle": False,
            "document_type": _extract_doc_type(extraction_result),
            "statement_count": 0,
            "statement_numbers": [],
            "period_from": None,
            "period_to": None,
            "booking_count": None,
        }

    statement_rows = normalized.get("kontoauszuege")
    period = normalized.get("zeitraum") if isinstance(normalized.get("zeitraum"), dict) else {}
    booking_count = normalized.get("booking_count")
    statement_numbers = _normalize_statement_numbers(normalized.get("auszugsnummer"))

    if isinstance(statement_rows, list) and statement_rows:
        aggregated_statement_numbers: list[str] = []
        aggregated_booking_count = 0
        derived_from: list[str] = []
        derived_to: list[str] = []

        for statement in statement_rows:
            if not isinstance(statement, dict):
                continue
            aggregated_statement_numbers.extend(_normalize_statement_numbers(statement.get("auszugsnummer")))
            buchungen = statement.get("buchungen")
            if isinstance(buchungen, list):
                aggregated_booking_count += len(buchungen)
            statement_period = statement.get("zeitraum")
            if isinstance(statement_period, dict):
                if _is_nonempty_string(statement_period.get("von")):
                    derived_from.append(str(statement_period["von"]).strip())
                if _is_nonempty_string(statement_period.get("bis")):
                    derived_to.append(str(statement_period["bis"]).strip())

        if not statement_numbers:
            statement_numbers = aggregated_statement_numbers
        if booking_count in (None, "") and aggregated_booking_count:
            booking_count = aggregated_booking_count

        period_from = period.get("von") if isinstance(period, dict) else None
        period_to = period.get("bis") if isinstance(period, dict) else None
        if not _is_nonempty_string(period_from) and derived_from:
            period_from = min(derived_from)
        if not _is_nonempty_string(period_to) and derived_to:
            period_to = max(derived_to)

        return {
            "is_bundle": True,
            "document_type": _extract_doc_type(extraction_result),
            "statement_count": len([item for item in statement_rows if isinstance(item, dict)]),
            "statement_numbers": statement_numbers,
            "period_from": period_from,
            "period_to": period_to,
            "booking_count": booking_count,
        }

    return {
        "is_bundle": False,
        "document_type": _extract_doc_type(extraction_result),
        "statement_count": 1,
        "statement_numbers": statement_numbers,
        "period_from": period.get("von") if isinstance(period, dict) else None,
        "period_to": period.get("bis") if isinstance(period, dict) else None,
        "booking_count": booking_count,
    }


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
        contract_issues = validate_canonical_extraction_result(extraction_result)
        if contract_issues:
            return {
                "success": False,
                "error": (
                    "Extraction result violates canonical Daigestr contract: "
                    + "; ".join(issue["field"] for issue in contract_issues)
                ),
                "data": sanitize_for_json({"violations": contract_issues}),
                "duration": time.monotonic() - start,
            }
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
                    "document_shape": _statement_bundle_summary(extraction_result),
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
