"""Helper Registry — register and retrieve DB-backed helpers."""
from __future__ import annotations

import hashlib
from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from typing import Any, Optional
from uuid import uuid4

from brix.db import BrixDB


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class HelperEntry:
    """Registry entry for a pipeline helper script."""

    name: str
    script: str = ""
    description: str = ""
    requirements: list[str] = field(default_factory=list)
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    code: str = ""
    content_hash: str = ""
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    id: Optional[str] = None  # stable UUID

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "HelperEntry":
        return cls(
            name=data["name"],
            script=data.get("script", ""),
            description=data.get("description", ""),
            requirements=data.get("requirements", []),
            input_schema=data.get("input_schema", {}),
            output_schema=data.get("output_schema", {}),
            code=data.get("code", ""),
            content_hash=data.get("content_hash", ""),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            id=data.get("id"),
        )


class HelperRegistry:
    """Persistent registry for DB-backed helper scripts."""

    def __init__(self, registry_path=None, db: Optional["BrixDB"] = None) -> None:
        # Shared BrixDB instance (or default central DB).
        self._db = db if db is not None else BrixDB()

    def _db_helper_to_entry(self, row: dict) -> HelperEntry:
        """Convert a DB helper row to a HelperEntry."""
        return HelperEntry(
            name=row["name"],
            script=row.get("script_path", ""),
            description=row.get("description", ""),
            requirements=row.get("requirements", []),
            input_schema=row.get("input_schema", {}),
            output_schema=row.get("output_schema", {}),
            code=row.get("code", ""),
            content_hash=row.get("content_hash", ""),
            created_at=row.get("created_at"),
            updated_at=row.get("updated_at"),
            id=row.get("id"),
        )

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------

    def register(
        self,
        name: str,
        script: str = "",
        description: str = "",
        requirements: Optional[list[str]] = None,
        input_schema: Optional[dict] = None,
        output_schema: Optional[dict] = None,
        code: str = "",
    ) -> HelperEntry:
        """Register or replace a helper entry.
        """
        now = _now_iso()
        existing_db = self._db.get_helper(name)
        existing = existing_db or {}

        created_at = (existing.get("created_at") if existing else None) or now
        stable_id = (existing.get("id") if existing else None) or str(uuid4())
        content_hash = hashlib.sha256(code.encode("utf-8")).hexdigest()

        if existing_db:
            old_code = self._db.get_helper_code(name) or ""
            self._db.record_object_version(
                obj_type="helper",
                name=name,
                content={"code": old_code, "meta": existing_db},
            )
            self._db.trim_object_versions("helper", name, keep=10)

        entry = HelperEntry(
            name=name,
            script=script or "",
            description=description,
            requirements=requirements or [],
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            code=code,
            content_hash=content_hash,
            created_at=created_at,
            updated_at=now,
            id=stable_id,
        )

        # Write to DB
        self._db.upsert_helper(
            name=name,
            script_path=entry.script,
            description=description,
            requirements=requirements or [],
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            helper_id=stable_id,
            code=code,
            content_hash=content_hash,
        )
        return entry

    def get(self, name: str) -> Optional[HelperEntry]:
        """Retrieve a helper entry by name or UUID, or ``None`` if not found.

        DB only — no YAML fallback.
        """
        db_row = self._db.get_helper(name)
        if db_row is not None:
            return self._db_helper_to_entry(db_row)
        return None

    def list_all(self) -> list[HelperEntry]:
        """Return all registered helpers sorted by name. DB only."""
        db_helpers = self._db.list_helpers()
        entries = []
        for row in db_helpers:
            try:
                entries.append(self._db_helper_to_entry(row))
            except (KeyError, TypeError):
                continue
        return sorted(entries, key=lambda e: e.name)

    def search(self, query: str) -> list[HelperEntry]:
        """Search helpers by name or description (case-insensitive substring match)."""
        q = query.lower()
        return [
            e for e in self.list_all()
            if q in e.name.lower() or q in e.description.lower()
        ]

    def update(self, name: str, **fields: Any) -> HelperEntry:
        db_row = self._db.get_helper(name)
        if db_row is None:
            raise KeyError(f"Helper '{name}' not found in registry")

        if (
            "script" in fields and fields["script"] != db_row.get("script_path")
        ) or ("code" in fields and fields["code"] != (db_row.get("code") or "")):
            old_code = self._db.get_helper_code(name) or ""
            self._db.record_object_version(
                obj_type="helper",
                name=name,
                content={"code": old_code, "meta": db_row},
            )
            self._db.trim_object_versions("helper", name, keep=10)

        raw = {
            "name": name,
            "script": db_row.get("script_path", ""),
            "description": db_row.get("description", ""),
            "requirements": db_row.get("requirements", []),
            "input_schema": db_row.get("input_schema", {}),
            "output_schema": db_row.get("output_schema", {}),
            "code": db_row.get("code", ""),
            "content_hash": db_row.get("content_hash", ""),
            "created_at": db_row.get("created_at"),
            "updated_at": db_row.get("updated_at"),
            "id": db_row.get("id"),
        }

        allowed = {"script", "description", "requirements", "input_schema", "output_schema", "code"}
        for key, value in fields.items():
            if key in allowed:
                raw[key] = value

        raw["updated_at"] = _now_iso()
        raw["script"] = raw.get("script") or ""
        raw["content_hash"] = hashlib.sha256((raw.get("code") or "").encode("utf-8")).hexdigest()
        updated_entry = HelperEntry.from_dict(raw)

        # Keep DB in sync
        self._db.upsert_helper(
            name=updated_entry.name,
            script_path=updated_entry.script,
            description=updated_entry.description,
            requirements=updated_entry.requirements,
            input_schema=updated_entry.input_schema,
            output_schema=updated_entry.output_schema,
            helper_id=updated_entry.id,
            code=updated_entry.code,
            content_hash=updated_entry.content_hash,
        )
        return updated_entry

    def remove(self, name: str) -> bool:
        """Remove a helper from the DB registry."""
        return self._db.delete_helper(name)

    def get_code(self, name: str) -> Optional[str]:
        """Get the Python source code for a helper from the DB."""
        return self._db.get_helper_code(name)
