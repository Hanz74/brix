"""Helper Registry — register, discover, and validate pipeline helper scripts.

DB-First: Helper code is stored in brix.db (helpers.code column).
When a helper is executed, its code is written to a temp file, run, then cleaned up.
Filesystem registry.yaml is kept as fallback for backward compatibility.
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

import yaml

from brix.db import BrixDB


REGISTRY_PATH = Path.home() / ".brix" / "helpers" / "registry.yaml"


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()


@dataclass
class HelperEntry:
    """Registry entry for a pipeline helper script."""

    name: str
    script: str
    description: str = ""
    requirements: list[str] = field(default_factory=list)
    input_schema: dict[str, Any] = field(default_factory=dict)
    output_schema: dict[str, Any] = field(default_factory=dict)
    created_at: Optional[str] = None
    updated_at: Optional[str] = None
    id: Optional[str] = None  # stable UUID

    def to_dict(self) -> dict:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: dict) -> "HelperEntry":
        return cls(
            name=data["name"],
            script=data["script"],
            description=data.get("description", ""),
            requirements=data.get("requirements", []),
            input_schema=data.get("input_schema", {}),
            output_schema=data.get("output_schema", {}),
            created_at=data.get("created_at"),
            updated_at=data.get("updated_at"),
            id=data.get("id"),
        )


class HelperRegistry:
    """Persistent registry for Brix pipeline helper scripts.

    DB-First: reads from brix.db, falls back to registry.yaml.
    All mutations write-through to both DB and disk.
    """

    def __init__(self, registry_path: Path = None, db: Optional["BrixDB"] = None) -> None:
        self._path = Path(registry_path) if registry_path else REGISTRY_PATH
        self._path.parent.mkdir(parents=True, exist_ok=True)
        # Shared BrixDB instance (or default central DB).
        self._db = db if db is not None else BrixDB()

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    def _load(self) -> dict[str, dict]:
        """Load raw registry data from disk (fallback)."""
        if not self._path.exists():
            return {}
        try:
            data = yaml.safe_load(self._path.read_text()) or {}
            return data if isinstance(data, dict) else {}
        except Exception:
            return {}

    def _save(self, data: dict[str, dict]) -> None:
        """Persist raw registry data to disk."""
        self._path.parent.mkdir(parents=True, exist_ok=True)
        self._path.write_text(yaml.dump(data, default_flow_style=False, allow_unicode=True))

    def _db_helper_to_entry(self, row: dict) -> HelperEntry:
        """Convert a DB helper row to a HelperEntry."""
        return HelperEntry(
            name=row["name"],
            script=row.get("script_path", ""),
            description=row.get("description", ""),
            requirements=row.get("requirements", []),
            input_schema=row.get("input_schema", {}),
            output_schema=row.get("output_schema", {}),
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
        script: str,
        description: str = "",
        requirements: Optional[list[str]] = None,
        input_schema: Optional[dict] = None,
        output_schema: Optional[dict] = None,
        code: Optional[str] = None,
    ) -> HelperEntry:
        """Register or replace a helper entry.

        Parameters
        ----------
        name:
            Unique identifier used in pipeline steps (e.g. ``"parse_invoice"``).
        script:
            Absolute or relative path to the Python script.
        description:
            Human-readable description of what the helper does.
        requirements:
            List of PEP-508 package requirements the helper needs.
        input_schema:
            JSON Schema dict describing accepted input parameters.
        output_schema:
            JSON Schema dict describing the helper's output structure.
        code:
            Python source code of the helper (stored in DB).

        Returns
        -------
        HelperEntry
            The stored entry.
        """
        now = _now_iso()
        # Preserve created_at and id if the helper already exists
        existing_db = self._db.get_helper(name)
        all_data = self._load()
        existing_yaml = all_data.get(name, {})
        existing = existing_db or existing_yaml

        created_at = (existing.get("created_at") if existing else None) or now
        stable_id = (existing.get("id") if existing else None) or str(uuid4())

        # Archive old entry if an update is happening
        if existing_db and existing_db.get("script_path"):
            old_code = self._db.get_helper_code(name) or ""
            self._db.record_object_version(
                obj_type="helper",
                name=name,
                content={"code": old_code, "meta": existing_db},
            )
            self._db.trim_object_versions("helper", name, keep=10)
        elif existing_yaml and existing_yaml.get("script"):
            old_script_path = existing_yaml.get("script", "")
            try:
                old_code = Path(old_script_path).read_text(encoding="utf-8") if Path(old_script_path).exists() else ""
            except Exception:
                old_code = ""
            self._db.record_object_version(
                obj_type="helper",
                name=name,
                content={"code": old_code, "meta": existing_yaml},
            )
            self._db.trim_object_versions("helper", name, keep=10)

        # Read code from script file if not provided
        if code is None and script and Path(script).exists():
            try:
                code = Path(script).read_text(encoding="utf-8")
            except Exception:
                pass

        entry = HelperEntry(
            name=name,
            script=script,
            description=description,
            requirements=requirements or [],
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            created_at=created_at,
            updated_at=now,
            id=stable_id,
        )

        # Write to YAML registry (backward compat)
        data = self._load()
        data[name] = entry.to_dict()
        self._save(data)

        # Write to DB
        self._db.upsert_helper(
            name=name,
            script_path=script,
            description=description,
            requirements=requirements or [],
            input_schema=input_schema or {},
            output_schema=output_schema or {},
            helper_id=stable_id,
            code=code,
        )
        return entry

    def get(self, name: str) -> Optional[HelperEntry]:
        """Retrieve a helper entry by name or UUID, or ``None`` if not found.

        DB-first, with YAML registry as fallback.
        """
        # Try DB first
        db_row = self._db.get_helper(name)
        if db_row is not None:
            return self._db_helper_to_entry(db_row)

        # Fallback: YAML registry
        data = self._load()
        raw = data.get(name)
        if raw is not None:
            return HelperEntry.from_dict(raw)
        # UUID fallback in YAML
        for entry_data in data.values():
            if isinstance(entry_data, dict) and entry_data.get("id") == name:
                return HelperEntry.from_dict(entry_data)
        return None

    def list_all(self) -> list[HelperEntry]:
        """Return all registered helpers sorted by name. DB-first."""
        # Try DB first
        db_helpers = self._db.list_helpers()
        if db_helpers:
            entries = []
            for row in db_helpers:
                try:
                    entries.append(self._db_helper_to_entry(row))
                except (KeyError, TypeError):
                    continue
            return sorted(entries, key=lambda e: e.name)

        # Fallback: YAML
        data = self._load()
        entries = []
        for raw in data.values():
            try:
                entries.append(HelperEntry.from_dict(raw))
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
        """Update one or more fields of an existing helper entry."""
        # Check DB first
        db_row = self._db.get_helper(name)
        if db_row is None:
            # Check YAML
            data = self._load()
            if name not in data:
                raise KeyError(f"Helper '{name}' not found in registry")

        if db_row:
            # Archive old version when script path or code is changing
            if "script" in fields and fields["script"] != db_row.get("script_path"):
                old_code = self._db.get_helper_code(name) or ""
                self._db.record_object_version(
                    obj_type="helper",
                    name=name,
                    content={"code": old_code, "meta": db_row},
                )
                self._db.trim_object_versions("helper", name, keep=10)

        # Also update YAML
        data = self._load()
        raw = data.get(name, {})
        if db_row and not raw:
            # Reconstruct raw from DB
            raw = {
                "name": name,
                "script": db_row.get("script_path", ""),
                "description": db_row.get("description", ""),
                "requirements": db_row.get("requirements", []),
                "input_schema": db_row.get("input_schema", {}),
                "output_schema": db_row.get("output_schema", {}),
                "created_at": db_row.get("created_at"),
                "updated_at": db_row.get("updated_at"),
                "id": db_row.get("id"),
            }

        allowed = {"script", "description", "requirements", "input_schema", "output_schema"}
        code = fields.pop("code", None)
        for key, value in fields.items():
            if key in allowed:
                raw[key] = value

        raw["updated_at"] = _now_iso()

        data[name] = raw
        self._save(data)
        updated_entry = HelperEntry.from_dict(raw)

        # Read code from new script file if script changed and no explicit code
        if code is None and "script" in fields and Path(fields["script"]).exists():
            try:
                code = Path(fields["script"]).read_text(encoding="utf-8")
            except Exception:
                pass

        # Keep DB in sync
        self._db.upsert_helper(
            name=updated_entry.name,
            script_path=updated_entry.script,
            description=updated_entry.description,
            requirements=updated_entry.requirements,
            input_schema=updated_entry.input_schema,
            output_schema=updated_entry.output_schema,
            helper_id=updated_entry.id,
            code=code,
        )
        return updated_entry

    def remove(self, name: str) -> bool:
        """Remove a helper from the registry and DB."""
        removed = False
        # Remove from YAML
        data = self._load()
        if name in data:
            del data[name]
            self._save(data)
            removed = True
        # Remove from DB
        if self._db.delete_helper(name):
            removed = True
        return removed

    def get_code(self, name: str) -> Optional[str]:
        """Get the Python source code for a helper. DB-first, filesystem fallback."""
        # Try DB
        code = self._db.get_helper_code(name)
        if code:
            return code

        # Fallback: read from script file
        entry = self.get(name)
        if entry and entry.script and Path(entry.script).exists():
            try:
                return Path(entry.script).read_text(encoding="utf-8")
            except Exception:
                pass
        return None
