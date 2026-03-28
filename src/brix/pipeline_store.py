"""Pipeline persistence: save, load, list, version.

DB-First: Pipeline YAML content is stored in brix.db (pipelines.yaml_content).
Filesystem is used as fallback for backward compatibility and for writing
YAML files (which are then synced to DB).
"""
import yaml
from datetime import datetime, timezone
from pathlib import Path
from typing import Optional

from brix.db import BrixDB
from brix.loader import PipelineLoader
from brix.models import Pipeline


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

    def save(self, pipeline_data: dict, name: Optional[str] = None) -> Path:
        """Save pipeline data as YAML to pipelines_dir and DB. Returns the file path.

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

        # Write to filesystem
        with open(path, "w") as f:
            f.write(yaml_content)

        # Write to DB (requirements extracted from pipeline data)
        requirements = pipeline_data.get("requirements", [])
        if not isinstance(requirements, list):
            requirements = []
        self._db.upsert_pipeline(
            name=pipeline_name,
            path=str(path),
            requirements=requirements,
            yaml_content=yaml_content,
        )

        return path

    def load(self, name: str) -> Pipeline:
        """Load a pipeline by name.

        Filesystem-first when file exists (ensures fresh writes are picked up),
        then falls back to DB content for pipelines not on disk.
        """
        # Try filesystem first
        for search_dir in self.search_paths:
            for ext in [".yaml", ".yml"]:
                path = Path(search_dir) / f"{name}{ext}"
                if path.exists():
                    return self.loader.load(str(path))

        # Fallback: DB
        yaml_content = self._db.get_pipeline_yaml_content(name)
        if yaml_content:
            return self.loader.load_from_string(yaml_content)

        raise FileNotFoundError(
            f"Pipeline '{name}' not found in filesystem or DB: {[str(p) for p in self.search_paths]}"
        )

    def load_raw(self, name: str) -> dict:
        """Load pipeline as raw dict (for inspection/modification).

        Filesystem-first for load_raw to ensure freshly written files
        are picked up immediately (important for MCP handlers that write
        directly to filesystem). Falls back to DB if not on filesystem.
        """
        # Try filesystem first (ensures fresh writes are picked up)
        for search_dir in self.search_paths:
            for ext in [".yaml", ".yml"]:
                path = Path(search_dir) / f"{name}{ext}"
                if path.exists():
                    with open(path) as f:
                        return yaml.safe_load(f) or {}

        # Fallback: DB
        yaml_content = self._db.get_pipeline_yaml_content(name)
        if yaml_content:
            return yaml.safe_load(yaml_content) or {}

        raise FileNotFoundError(f"Pipeline '{name}' not found")

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
        """List pipelines from filesystem first, then DB for any not found on disk.

        Filesystem takes precedence (fresh writes), DB provides additional pipelines
        that don't have corresponding files.
        """
        seen: set[str] = set()
        results = []

        # Primary: scan filesystem (search_paths)
        for search_dir in self.search_paths:
            search_dir = Path(search_dir)
            if not search_dir.exists():
                continue
            files = sorted(search_dir.glob("*.yaml")) + sorted(search_dir.glob("*.yml"))
            for f in files:
                if f.stem in seen:
                    continue
                seen.add(f.stem)
                try:
                    pipeline = self.loader.load(str(f))
                    results.append({
                        "name": pipeline.name,
                        "version": pipeline.version,
                        "description": pipeline.description or "",
                        "steps": len(pipeline.steps),
                        "path": str(f),
                    })
                except Exception as e:
                    results.append({
                        "name": f.stem,
                        "version": "?",
                        "description": f"Error: {e}",
                        "steps": 0,
                        "path": str(f),
                    })

        # Secondary: DB pipelines not on filesystem (DB-only content)
        db_pipelines = self._db.list_pipelines()
        for p in db_pipelines:
            name = p["name"]
            if name in seen:
                continue
            seen.add(name)
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
