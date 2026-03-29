"""TriggerStore — DB-backed CRUD for trigger configurations.

Stores triggers in brix.db (BrixDB).
The TriggerService continues to read from triggers.yaml for backward compat;
TriggerStore is the new MCP-facing layer.
"""
from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

_VALID_TYPES = {"mail", "file", "http_poll", "pipeline_done", "event"}  # "event" added T-BRIX-DB-22


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


class TriggerStore:
    """CRUD storage for trigger configurations in brix.db."""

    def __init__(
        self,
        db: Optional[Any] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        if db is not None:
            self._db = db
        else:
            from brix.db import BrixDB, BRIX_DB_PATH
            path = Path(db_path) if db_path else BRIX_DB_PATH
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = BrixDB(db_path=path)
        # db_path kept for compatibility
        self.db_path = self._db.db_path

    # ------------------------------------------------------------------
    # CRUD (delegates to BrixDB)
    # ------------------------------------------------------------------

    def add(
        self,
        name: str,
        type: str,
        config: dict,
        pipeline: str,
        enabled: bool = True,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> dict:
        """Add a new trigger. Raises ValueError on invalid type or duplicate name."""
        if type not in _VALID_TYPES:
            raise ValueError(
                f"Unknown trigger type '{type}'. Valid types: {sorted(_VALID_TYPES)}"
            )
        return self._db.trigger_add(
            name=name,
            type=type,
            config=config,
            pipeline=pipeline,
            enabled=enabled,
            project=project,
            tags=tags,
            group_name=group_name,
        )

    def list_all(self) -> list[dict]:
        """Return all triggers sorted by name."""
        return self._db.trigger_list()

    def get(self, name: str) -> Optional[dict]:
        """Get a trigger by name or UUID. Returns None if not found."""
        return self._db.trigger_get(name)

    def update(
        self,
        name: str,
        config: Optional[dict] = None,
        enabled: Optional[bool] = None,
        pipeline: Optional[str] = None,
        project: Optional[str] = None,
        tags: Optional[list] = None,
        group_name: Optional[str] = None,
    ) -> Optional[dict]:
        """Partially update a trigger. Returns updated dict or None if not found."""
        return self._db.trigger_update(
            name=name,
            config=config,
            enabled=enabled,
            pipeline=pipeline,
            project=project,
            tags=tags,
            group_name=group_name,
        )

    def delete(self, name: str) -> bool:
        """Delete a trigger by name or UUID. Returns True if deleted."""
        return self._db.trigger_delete(name)

    def record_fired(
        self,
        name: str,
        run_id: Optional[str] = None,
        status: str = "fired",
    ) -> None:
        """Update last_fired_at, last_run_id, last_status after a trigger fires."""
        self._db.trigger_record_fired(name=name, run_id=run_id, status=status)


class TriggerGroupStore:
    """CRUD storage for trigger groups in brix.db (T-BRIX-V6-20)."""

    def __init__(
        self,
        db: Optional[Any] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        if db is not None:
            self._db = db
        else:
            from brix.db import BrixDB, BRIX_DB_PATH
            path = Path(db_path) if db_path else BRIX_DB_PATH
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = BrixDB(db_path=path)
        # db_path kept for compatibility
        self.db_path = self._db.db_path

    def add(
        self,
        name: str,
        triggers: list[str],
        description: str = "",
        enabled: bool = True,
    ) -> dict:
        """Add a new trigger group. Raises ValueError on duplicate name."""
        return self._db.trigger_group_add(
            name=name,
            triggers=triggers,
            description=description,
            enabled=enabled,
        )

    def list_all(self) -> list[dict]:
        """Return all trigger groups sorted by name."""
        return self._db.trigger_group_list()

    def get(self, name: str) -> Optional[dict]:
        """Get a trigger group by name or UUID. Returns None if not found."""
        return self._db.trigger_group_get(name)

    def update(
        self,
        name: str,
        triggers: Optional[list[str]] = None,
        description: Optional[str] = None,
        enabled: Optional[bool] = None,
    ) -> Optional[dict]:
        """Partially update a trigger group. Returns updated dict or None if not found."""
        return self._db.trigger_group_update(
            name=name,
            triggers=triggers,
            description=description,
            enabled=enabled,
        )

    def delete(self, name: str) -> bool:
        """Delete a trigger group by name or UUID. Returns True if deleted."""
        return self._db.trigger_group_delete(name)
