"""Pipeline persistence: save, load, list, version.

DB-first storage with dual-write rollback support.
"""
import json
import logging
import os
import sqlite3
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional

from brix.db import BrixDB, _PIPELINE_BOOL_COLUMNS, _PIPELINE_JSON_COLUMNS, _json_dumps
from brix.loader import PipelineLoader
from brix.models import Pipeline

logger = logging.getLogger(__name__)


def _now_iso() -> str:
    """Return current UTC time as ISO-8601 string."""
    return datetime.now(timezone.utc).isoformat()

DEFAULT_PIPELINES_DIR = Path.home() / ".brix" / "pipelines"

DEFAULT_SEARCH_PATHS = [
    Path.home() / ".brix" / "pipelines",  # User-Pipelines
    Path("/app/pipelines"),                 # Container Volume-Mount
]


class PipelineStore:
    """Manages pipeline storage — DB-first with filesystem fallback.

    Primary storage: brix.db (pipelines table, yaml_content column).
    Fallback: filesystem search paths (for backward compatibility).
    Saves always write to both DB and filesystem.
    """

    def __init__(
        self,
        pipelines_dir: Optional[Path] = None,
        search_paths: Optional[list] = None,
        db: Optional[BrixDB] = None,
    ):
        self.pipelines_dir = Path(pipelines_dir) if pipelines_dir is not None else DEFAULT_PIPELINES_DIR
        self.pipelines_dir.mkdir(parents=True, exist_ok=True)
        if search_paths is not None:
            # Caller provided explicit search_paths — use as-is
            self.search_paths = [Path(p) for p in search_paths]
        elif pipelines_dir is not None:
            # Custom pipelines_dir: prepend it to the default search paths
            extra = [Path(pipelines_dir)]
            self.search_paths = extra + [
                p for p in DEFAULT_SEARCH_PATHS if Path(p) != Path(pipelines_dir)
            ]
        else:
            self.search_paths = DEFAULT_SEARCH_PATHS
        self.loader = PipelineLoader()
        # Shared BrixDB instance (or default central DB).
        self._db = db if db is not None else BrixDB()

    def _step_source_mode(self) -> str:
        mode = (os.environ.get("BRIX_STEP_SOURCE") or "db").strip().lower()
        return mode if mode in {"db", "yaml", "dual"} else "db"

    def _load_pipeline_from_yaml(self, yaml_content: str, name: str) -> Pipeline:
        if yaml_content:
            return self.loader.load_from_string(yaml_content)
        raise FileNotFoundError(f"Pipeline '{name}' not found in DB")

    def _load_raw_from_yaml(self, yaml_content: str, name: str) -> dict:
        if yaml_content:
            return yaml.safe_load(yaml_content) or {}
        raise FileNotFoundError(f"Pipeline '{name}' not found in DB")

    def _load_pipeline_from_db_rows(self, pipeline_row: dict, name: str) -> Pipeline:
        pipeline_id = pipeline_row["id"]
        with self._db._connect() as conn:
            conn.row_factory = sqlite3.Row
            step_rows = [
                {key: value for key, value in dict(row).items() if value is not None}
                for row in conn.execute(
                    "SELECT * FROM pipeline_step WHERE pipeline_id=? ORDER BY position ASC",
                    (pipeline_id,),
                ).fetchall()
            ]
        return Pipeline.from_db(
            pipeline_row,
            step_rows,
            self._db.get_pipeline_credentials(pipeline_id),
            self._db.get_pipeline_inputs(pipeline_id),
        )

    def _load_raw_from_db_rows(self, pipeline_row: dict) -> dict:
        pipeline_id = pipeline_row["id"]
        raw = self._db.pipeline_to_dict(pipeline_id)
        if raw is None:
            raise FileNotFoundError(f"Pipeline '{pipeline_row['name']}' not found in DB")
        return raw

    def _validate_dual_read(self, pipeline_row: dict, yaml_content: str) -> None:
        if not yaml_content:
            logger.warning(
                "dual read validation skipped for pipeline '%s': missing yaml_content",
                pipeline_row["name"],
            )
            return
        db_dict = self._load_raw_from_db_rows(pipeline_row)
        yaml_dict = yaml.safe_load(yaml_content) or {}
        if db_dict != yaml_dict:
            raise ValueError(
                f"Pipeline '{pipeline_row['name']}' DB rows do not match yaml_content"
            )

    def _pipeline_metadata_updates_from_raw(
        self,
        raw: dict[str, Any],
        pipeline_row: dict,
    ) -> dict[str, Any]:
        updates: dict[str, Any] = {
            "version": raw.get("version") or pipeline_row.get("version") or "1.0.0",
            "brix_version": raw.get("brix_version", pipeline_row.get("brix_version")),
            "kind": raw.get("kind", pipeline_row.get("kind")),
            "extends": raw.get("extends", pipeline_row.get("extends")),
            "idempotency_key": raw.get("idempotency_key", pipeline_row.get("idempotency_key")),
            "description": raw.get("description", pipeline_row.get("description") or ""),
            "project": raw.get("project", pipeline_row.get("project") or ""),
            "group_name": raw.get("group", pipeline_row.get("group_name") or ""),
        }

        raw_tags = raw.get("tags", pipeline_row.get("tags") or [])
        if isinstance(raw_tags, str):
            try:
                raw_tags = json.loads(raw_tags)
            except Exception:
                raw_tags = []
        updates["tags"] = _json_dumps(raw_tags if isinstance(raw_tags, list) else [])

        bool_defaults = {
            "is_template": False,
            "compositor_mode": False,
            "allow_code": True,
            "strict_bricks": False,
            "test_mode": False,
        }
        for column, field in _PIPELINE_BOOL_COLUMNS.items():
            value = raw.get(field)
            if value is None:
                value = pipeline_row.get(column)
            if value is None:
                value = bool_defaults[field]
            updates[column] = int(bool(value))

        json_defaults = {
            "template_params_json": {},
            "blueprint_params_json": [],
            "error_handling_json": {},
            "retry_profiles_json": {},
            "notify_json": {},
            "groups_json": {},
            "output_json": None,
            "output_slots_json": {},
            "requirements_json": [],
        }
        for column, field in _PIPELINE_JSON_COLUMNS.items():
            value = raw.get(field)
            if value is None:
                existing = pipeline_row.get(column)
                if existing not in (None, ""):
                    updates[column] = existing
                    continue
                value = json_defaults[column]
            updates[column] = None if value is None else _json_dumps(value)

        return updates

    def save(self, pipeline_data: dict, name: Optional[str] = None) -> Path:
        """Save pipeline data to DB with yaml_content dual-write. Returns the virtual path.

        Automatically manages created_at / updated_at timestamps:
        - created_at is set on first save; preserved on subsequent saves.
        - updated_at is refreshed on every save.

        Before overwriting an existing pipeline the old YAML content is archived
        as an object version in brix.db (retention: last 10 versions).
        """
        pipeline_name = name or pipeline_data.get("name", "unnamed")
        filename = f"{pipeline_name}.yaml"
        path = self.pipelines_dir / filename

        now = _now_iso()
        # Preserve created_at: check DB first, then file
        existing_content = self._db.get_pipeline_yaml_content(pipeline_name)
        if existing_content:
            try:
                existing = yaml.safe_load(existing_content) or {}
                pipeline_data.setdefault("created_at", existing.get("created_at") or now)
                # Archive the *current* content before overwriting
                self._db.record_object_version(
                    obj_type="pipeline",
                    name=pipeline_name,
                    content=existing,
                )
                self._db.trim_object_versions("pipeline", pipeline_name, keep=10)
            except Exception:
                pipeline_data.setdefault("created_at", now)
        elif path.exists():
            try:
                with open(path) as f:
                    existing = yaml.safe_load(f) or {}
                pipeline_data.setdefault("created_at", existing.get("created_at") or now)
                self._db.record_object_version(
                    obj_type="pipeline",
                    name=pipeline_name,
                    content=existing,
                )
                self._db.trim_object_versions("pipeline", pipeline_name, keep=10)
            except Exception:
                pipeline_data.setdefault("created_at", now)
        else:
            pipeline_data.setdefault("created_at", now)
        pipeline_data["updated_at"] = now

        yaml_content = yaml.dump(pipeline_data, default_flow_style=False, allow_unicode=True)
        requirements = pipeline_data.get("requirements", [])
        if not isinstance(requirements, list):
            requirements = []

        pipeline_id = self._db.upsert_pipeline(
            name=pipeline_name,
            path=str(path),
            requirements=requirements,
            yaml_content=yaml_content,
        )

        pipeline_row = self._db.get_pipeline(pipeline_name)
        if pipeline_row is None:
            raise FileNotFoundError(f"Pipeline '{pipeline_name}' not found in DB after save")

        with self._db._connect() as conn:
            conn.row_factory = sqlite3.Row
            conn.execute("DELETE FROM pipeline_step WHERE pipeline_id=?", (pipeline_id,))
            conn.execute("DELETE FROM pipeline_credential WHERE pipeline_id=?", (pipeline_id,))
            conn.execute("DELETE FROM pipeline_input WHERE pipeline_id=?", (pipeline_id,))

            for step_order, step in enumerate(pipeline_data.get("steps") or []):
                self._db.upsert_step(pipeline_id, step, step_order=step_order, conn=conn)

            credentials = pipeline_data.get("credentials") or {}
            for alias, credential in credentials.items():
                if isinstance(credential, str):
                    env_ref = credential
                    refresh = None
                else:
                    env_ref = (credential or {}).get("env") or ""
                    refresh = (credential or {}).get("refresh")
                self._db.upsert_pipeline_credential(
                    pipeline_id,
                    alias,
                    env_ref,
                    refresh=refresh,
                    conn=conn,
                )

            pipeline_input = pipeline_data.get("input") or {}
            for input_key, input_spec in pipeline_input.items():
                input_spec = input_spec or {}
                self._db.upsert_pipeline_input(
                    pipeline_id,
                    input_key,
                    input_spec.get("type") or "string",
                    default_value=input_spec.get("default"),
                    description=input_spec.get("description"),
                    conn=conn,
                )

            metadata_updates = self._pipeline_metadata_updates_from_raw(pipeline_data, pipeline_row)
            metadata_updates["yaml_content"] = yaml_content
            metadata_updates["migration_status"] = "v71_complete"
            metadata_updates["updated_at"] = now
            assignments = ", ".join(f"{column}=?" for column in metadata_updates)
            conn.execute(
                f"UPDATE pipeline SET {assignments} WHERE id=?",
                [*metadata_updates.values(), pipeline_id],
            )

        return path

    def load(self, name: str) -> Pipeline:
        """Load a pipeline by name from DB rows or yaml_content."""
        pipeline_row = self._db.get_pipeline(name)
        if pipeline_row is None:
            raise FileNotFoundError(f"Pipeline '{name}' not found in DB")

        yaml_content = pipeline_row.get("yaml_content") or self._db.get_pipeline_yaml_content(name) or ""
        mode = self._step_source_mode()

        if mode == "yaml":
            return self._load_pipeline_from_yaml(yaml_content, name)

        if pipeline_row.get("migration_status") == "v71_complete":
            if mode == "dual":
                self._validate_dual_read(pipeline_row, yaml_content)
            return self._load_pipeline_from_db_rows(pipeline_row, name)

        return self._load_pipeline_from_yaml(yaml_content, name)

    def load_raw(self, name: str) -> dict:
        """Load pipeline as raw dict from DB rows or yaml_content."""
        pipeline_row = self._db.get_pipeline(name)
        if pipeline_row is None:
            raise FileNotFoundError(f"Pipeline '{name}' not found in DB")

        yaml_content = pipeline_row.get("yaml_content") or self._db.get_pipeline_yaml_content(name) or ""
        mode = self._step_source_mode()

        if mode == "yaml":
            return self._load_raw_from_yaml(yaml_content, name)

        if pipeline_row.get("migration_status") == "v71_complete":
            if mode == "dual":
                self._validate_dual_read(pipeline_row, yaml_content)
            return self._load_raw_from_db_rows(pipeline_row)

        return self._load_raw_from_yaml(yaml_content, name)

    def exists(self, name: str) -> bool:
        """Check if a pipeline exists in DB or any search path."""
        # Check DB first
        if self._db.get_pipeline(name) is not None:
            return True
        # Fallback: filesystem
        for search_dir in self.search_paths:
            if any(
                (Path(search_dir) / f"{name}{ext}").exists()
                for ext in [".yaml", ".yml"]
            ):
                return True
        return False

    def list_all(self) -> list[dict]:
        """List all pipelines from DB only.

        The filesystem is no longer scanned; all pipelines are read from the
        DB (yaml_content column).  This ensures a single authoritative source
        and avoids returning test-only files that live in mounted directories.
        """
        results = []
        db_pipelines = self._db.list_pipelines()
        for p in db_pipelines:
            name = p["name"]
            yaml_content = self._db.get_pipeline_yaml_content(name)
            if yaml_content:
                try:
                    pipeline = self.loader.load_from_string(yaml_content)
                    results.append({
                        "name": pipeline.name,
                        "version": pipeline.version,
                        "description": pipeline.description or "",
                        "steps": len(pipeline.steps),
                        "path": p.get("path", ""),
                    })
                except Exception as e:
                    results.append({
                        "name": name,
                        "version": "?",
                        "description": f"Error: {e}",
                        "steps": 0,
                        "path": p.get("path", ""),
                    })
            else:
                # Pipeline row exists but has no yaml_content — skip (no content to show)
                pass

        return results

    def delete(self, name: str) -> bool:
        """Delete a pipeline from DB and pipelines_dir. Returns True if deleted."""
        deleted = False
        # Delete from filesystem
        for ext in [".yaml", ".yml"]:
            path = self.pipelines_dir / f"{name}{ext}"
            if path.exists():
                path.unlink()
                deleted = True
        # Delete from DB
        if self._db.delete_pipeline(name):
            deleted = True
        return deleted

    def get_version(self, name: str) -> str:
        """Get the current version of a pipeline."""
        pipeline = self.load(name)
        return pipeline.version

    def find_by_id(self, pipeline_id: str) -> Optional[str]:
        """Find a pipeline name by its stable UUID.

        Scans all pipelines and returns the name of the first match, or None.
        """
        for info in self.list_all():
            try:
                raw = self.load_raw(info["name"])
            except Exception:
                continue
            if raw.get("id") == pipeline_id:
                return info["name"]
        return None

    def resolve(self, name_or_id: str) -> str:
        """Resolve a pipeline name or UUID to a canonical pipeline name.

        Tries name first (exact match), then UUID lookup.
        Raises FileNotFoundError if not found by either method.
        """
        if self.exists(name_or_id):
            return name_or_id
        # Try UUID lookup
        found_name = self.find_by_id(name_or_id)
        if found_name:
            return found_name
        raise FileNotFoundError(
            f"Pipeline '{name_or_id}' not found by name or id in: "
            f"{[str(p) for p in self.search_paths]}"
        )
