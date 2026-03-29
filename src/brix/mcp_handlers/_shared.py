"""Shared utilities for MCP handler modules.

This module provides shared state, utility functions, and imports that are
used across multiple handler modules. Import from here to avoid circular
imports and ensure all handlers use the same singletons.
"""
from __future__ import annotations

import json
import asyncio
from pathlib import Path

import yaml

from brix.bricks.registry import BrickRegistry
from brix.db import BrixDB
from brix.helper_registry import HelperRegistry
from brix.loader import PipelineLoader
from brix.validator import PipelineValidator
from brix.history import RunHistory
from brix.engine import PipelineEngine
from brix.mcp_pool import McpConnectionPool
from brix.pipeline_store import PipelineStore
from brix.credential_store import CredentialStore, CredentialNotFoundError, CREDENTIAL_TYPES
from brix.config import config

# Shared singletons
_registry = BrickRegistry()
_loader = PipelineLoader()
_validator = PipelineValidator()
_store = PipelineStore()
_audit_db = BrixDB()  # shared instance for audit logging

# Background-run registry — maps run_id → asyncio.Task for async-mode runs
_background_runs: dict[str, "asyncio.Task[None]"] = {}

# Schema-consultation tracking — maps source_key → {brick_name: timestamp}
# Used by T-BRIX-V8-09 to warn when add_step is called without prior get_brick_schema
import time as _time_mod
_schema_consultations: dict[str, dict[str, float]] = {}
# TTL in seconds for schema consultation entries
_SCHEMA_CONSULTATION_TTL_SECONDS: int = config.SCHEMA_CONSULTATION_TTL_SECONDS

# Auto-kill timeout: cancel background runs whose last heartbeat is older than this
BACKGROUND_RUN_TIMEOUT_SECONDS: int = config.BACKGROUND_RUN_TIMEOUT_SECONDS

# Watchdog check interval in seconds
_WATCHDOG_INTERVAL_SECONDS: int = config.WATCHDOG_INTERVAL_SECONDS

# Module-level reference to the watchdog task — kept alive for the server lifetime
_watchdog_task: "asyncio.Task[None] | None" = None

# Default pipeline directory (primary save target, kept for backward compat)
PIPELINE_DIR = Path.home() / ".brix" / "pipelines"


def _pipeline_dir() -> Path:
    """Return the primary pipeline directory, creating it if needed.

    Looks up brix.mcp_server.PIPELINE_DIR at call-time so that
    monkeypatch.setattr('brix.mcp_server.PIPELINE_DIR', ...) in tests
    takes effect even though handlers live in sub-modules.
    """
    import sys as _sys
    mcp_mod = _sys.modules.get("brix.mcp_server")
    effective = getattr(mcp_mod, "PIPELINE_DIR", PIPELINE_DIR) if mcp_mod is not None else PIPELINE_DIR
    effective.mkdir(parents=True, exist_ok=True)
    return effective


def _pipeline_path(name: str) -> Path:
    """Return the save path for a named pipeline YAML (always in pipelines_dir)."""
    return _pipeline_dir() / f"{name}.yaml"


def _now_iso_helper() -> str:
    """Return current UTC time as ISO-8601 string."""
    from datetime import datetime, timezone
    return datetime.now(timezone.utc).isoformat()


def _extract_source(arguments: dict) -> "dict | None":
    """Extract the 'source' dict from tool arguments (may be None)."""
    src = arguments.get("source")
    if isinstance(src, str):
        import warnings
        warnings.warn(
            "source must be {session, model, agent} object, not string. "
            f"Got string value: {src!r}. Ignoring source field.",
            stacklevel=2,
        )
        return None
    return src if isinstance(src, dict) else None


def _source_summary(source: "dict | None", **kwargs: str) -> str:
    """Build a compact arguments_summary string for audit_log."""
    parts = []
    for k, v in kwargs.items():
        if v:
            parts.append(f"{k}={v!r}")
    if source:
        if source.get("session"):
            parts.append(f"session={source['session']!r}")
    return ", ".join(parts)


def _load_pipeline_yaml(name: str) -> dict:
    """Load a pipeline YAML file as raw dict.

    Accepts pipeline name OR UUID (stable id). Creates a PipelineStore with
    the effective PIPELINE_DIR (resolved at call-time via _pipeline_dir()) so
    that monkeypatching brix.mcp_server.PIPELINE_DIR in tests works.
    """
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    resolved = store.resolve(name)
    return store.load_raw(resolved)


def _save_pipeline_yaml(name: str, data: dict) -> None:
    """Save a pipeline dict to YAML (via PipelineStore, which manages timestamps)."""
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    store.save(data, name)


def _validate_pipeline_dict(data: dict) -> dict:
    """Validate a pipeline dict using PipelineValidator. Returns validation summary."""
    try:
        pipeline = _loader.load_from_string(yaml.dump(data))
        result = _validator.validate(pipeline)
        return {
            "valid": result.is_valid,
            "errors": result.errors,
            "warnings": result.warnings,
            "checks": result.checks,
        }
    except Exception as exc:
        return {
            "valid": False,
            "errors": [str(exc)],
            "warnings": [],
            "checks": [],
        }


def _find_step_recursive(steps: list, step_id: str) -> "dict | None":
    """Search for a step by ID recursively through all nesting levels.

    Checks the given step list and recurses into:
    - repeat.sequence
    - choose.choices[].steps  and  choose.default_steps
    - parallel.sub_steps

    Returns the matching step dict or None.
    """
    for step in steps:
        if step.get("id") == step_id:
            return step
        # repeat -> sequence
        if "sequence" in step:
            found = _find_step_recursive(step["sequence"], step_id)
            if found is not None:
                return found
        # choose -> choices[].steps + default_steps
        if "choices" in step:
            for choice in step["choices"]:
                found = _find_step_recursive(choice.get("steps", []), step_id)
                if found is not None:
                    return found
        if "default_steps" in step:
            found = _find_step_recursive(step["default_steps"], step_id)
            if found is not None:
                return found
        # parallel -> sub_steps
        if "sub_steps" in step:
            found = _find_step_recursive(step["sub_steps"], step_id)
            if found is not None:
                return found
    return None


def _make_helper_dict(entry) -> dict:
    """Serialise a HelperEntry to a plain dict for MCP responses.

    Enriches the response with project/tags/group_name from the DB
    so that get_helper and list_helpers always return org fields.
    """
    import json as _json

    d: dict = {
        "name": entry.name,
        "script": entry.script,
        "description": entry.description,
        "requirements": entry.requirements,
        "input_schema": entry.input_schema,
        "output_schema": entry.output_schema,
    }
    if entry.id is not None:
        d["id"] = entry.id
    if entry.created_at is not None:
        d["created_at"] = entry.created_at
    if entry.updated_at is not None:
        d["updated_at"] = entry.updated_at

    # T-BRIX-ORG-01: enrich with project/tags/group from DB
    try:
        from brix.db import BrixDB as _BrixDB
        _db = _BrixDB()
        db_row = _db.get_helper(entry.name)
        if db_row:
            d["project"] = db_row.get("project", "") or ""
            raw_tags = db_row.get("tags", "[]")
            if isinstance(raw_tags, str):
                try:
                    d["tags"] = _json.loads(raw_tags)
                except (ValueError, TypeError):
                    d["tags"] = []
            else:
                d["tags"] = raw_tags if isinstance(raw_tags, list) else []
            d["group"] = db_row.get("group_name", "") or ""
        else:
            d["project"] = ""
            d["tags"] = []
            d["group"] = ""
    except Exception:
        d["project"] = ""
        d["tags"] = []
        d["group"] = ""
    return d


def _validate_python_code(code: str) -> "str | None":
    """Compile-check Python code. Returns None if valid, error message if not."""
    try:
        compile(code, "<create_helper>", "exec")
        return None
    except SyntaxError as exc:
        return f"SyntaxError at line {exc.lineno}: {exc.msg}"


def _managed_helper_dir() -> Path:
    """Return ~/.brix/helpers/ creating it if needed."""
    d = Path.home() / ".brix" / "helpers"
    d.mkdir(parents=True, exist_ok=True)
    return d


def _code_line_count(code: str) -> int:
    """Return number of lines in the given code string."""
    return len(code.splitlines())


def _normalize_name(name: str) -> str:
    """Normalize a helper/pipeline name for fuzzy comparison."""
    return name.lower().replace("_", "").replace("-", "")


def _name_similarity(a: str, b: str) -> float:
    """Compute similarity ratio between two normalized names via SequenceMatcher."""
    import difflib
    return difflib.SequenceMatcher(None, _normalize_name(a), _normalize_name(b)).ratio()


def _description_jaccard(desc_a: str, desc_b: str) -> float:
    """Compute Jaccard similarity between description token sets."""
    tokens_a = set(desc_a.lower().split())
    tokens_b = set(desc_b.lower().split())
    if not tokens_a or not tokens_b:
        return 0.0
    intersection = tokens_a & tokens_b
    union = tokens_a | tokens_b
    return len(intersection) / len(union)


def _find_similar_helpers(name: str, description: str) -> list[dict]:
    """Return list of similar existing helpers with similarity reason."""
    registry = HelperRegistry()
    existing = registry.list_all()
    similar: list[dict] = []
    for entry in existing:
        if entry.name == name:
            continue  # exact match is handled by overwrite logic
        name_sim = _name_similarity(name, entry.name)
        desc_sim = _description_jaccard(description, entry.description) if description and entry.description else 0.0
        if name_sim >= 0.7 or desc_sim >= 0.5:
            reasons = []
            if name_sim >= 0.7:
                reasons.append(f"Name-Match: {name_sim:.0%}")
            if desc_sim >= 0.5:
                reasons.append(f"Description-Overlap: {desc_sim:.0%}")
            similar.append({
                "name": entry.name,
                "reason": ", ".join(reasons),
            })
    return similar


def _find_similar_pipelines(name: str, description: str) -> list[dict]:
    """Return list of similar existing pipelines with similarity reason.

    Reads descriptions from raw YAML to avoid relying on Pipeline model
    validation (which rejects empty-step pipelines and corrupts the description).
    """
    import yaml as _yaml_mod
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    similar: list[dict] = []
    seen: set[str] = set()
    for search_dir in store.search_paths:
        search_dir_path = Path(search_dir)
        if not search_dir_path.exists():
            continue
        for f in sorted(search_dir_path.glob("*.yaml")) + sorted(search_dir_path.glob("*.yml")):
            existing_name = f.stem
            if existing_name in seen:
                continue
            seen.add(existing_name)
            if existing_name == name:
                continue
            # Read raw YAML to get description without model validation errors
            try:
                raw = _yaml_mod.safe_load(f.read_text()) or {}
                existing_desc = raw.get("description", "")
            except Exception:
                existing_desc = ""
            name_sim = _name_similarity(name, existing_name)
            desc_sim = _description_jaccard(description, existing_desc) if description and existing_desc else 0.0
            if name_sim >= 0.7 or desc_sim >= 0.5:
                reasons = []
                if name_sim >= 0.7:
                    reasons.append(f"Name-Match: {name_sim:.0%}")
                if desc_sim >= 0.5:
                    reasons.append(f"Description-Overlap: {desc_sim:.0%}")
                similar.append({
                    "name": existing_name,
                    "reason": ", ".join(reasons),
                })
    return similar


def _scan_pipelines_for_helper(helper_name: str) -> list[str]:
    """Return a list of pipeline names that reference the given helper name."""
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    affected: list[str] = []
    for info in store.list_all():
        try:
            raw = store.load_raw(info["name"])
        except Exception:
            continue
        yaml_text = yaml.dump(raw)
        if helper_name in yaml_text:
            affected.append(info["name"])
    return affected


def _scan_pipelines_for_sub_pipeline(pipeline_name: str) -> list[str]:
    """Return a list of pipeline names that reference the given name as a sub-pipeline."""
    store = PipelineStore(pipelines_dir=_pipeline_dir())
    affected: list[str] = []
    for info in store.list_all():
        if info["name"] == pipeline_name:
            continue  # skip self
        try:
            raw = store.load_raw(info["name"])
        except Exception:
            continue
        yaml_text = yaml.dump(raw)
        # Sub-pipeline references appear as `pipeline: <name>` in YAML
        if pipeline_name in yaml_text:
            affected.append(info["name"])
    return affected


def _re_module_name(error_msg: str) -> "str | None":
    """Extract the missing module name from a ModuleNotFoundError message."""
    import re
    m = re.search(r"No module named ['\"]([^'\"]+)['\"]", error_msg)
    return m.group(1) if m else None


def _ensure_watchdog() -> None:
    """Start the background watchdog task if it is not already running.

    Safe to call multiple times — idempotent within the same event loop.
    """
    global _watchdog_task
    try:
        loop = asyncio.get_running_loop()
    except RuntimeError:
        return  # No running event loop — nothing to do

    # Check if the existing task is still live on the *current* loop
    if _watchdog_task is not None and not _watchdog_task.done():
        try:
            if _watchdog_task.get_loop() is loop:
                return  # Already running on this loop — nothing to do
        except AttributeError:
            pass  # Task.get_loop() not available in older Python — fall through

    _watchdog_task = loop.create_task(_background_run_watchdog())


async def _background_run_watchdog() -> None:
    """Periodically cancel stale background runs."""
    import time as _time_mod
    from brix.context import WORKDIR_BASE

    while True:
        await asyncio.sleep(_WATCHDOG_INTERVAL_SECONDS)
        now = _time_mod.time()
        stale: list[str] = []
        for run_id, task in list(_background_runs.items()):
            if task.done():
                stale.append(run_id)
                continue
            run_json = WORKDIR_BASE / run_id / "run.json"
            try:
                import json as _json_mod
                with open(run_json) as fh:
                    meta = _json_mod.load(fh)
                heartbeat = meta.get("last_heartbeat", 0)
                age = now - heartbeat if heartbeat else now
                if age > BACKGROUND_RUN_TIMEOUT_SECONDS:
                    import logging
                    logging.getLogger(__name__).warning(
                        "Auto-kill: cancelling stale background run '%s' "
                        "(no heartbeat for %.0f s)",
                        run_id,
                        age,
                    )
                    task.cancel()
                    stale.append(run_id)
            except (FileNotFoundError, OSError, ValueError):
                pass

        for run_id in stale:
            _background_runs.pop(run_id, None)


# ---------------------------------------------------------------------------
# Schema-consultation helpers (T-BRIX-V8-09)
# ---------------------------------------------------------------------------

def _source_key(source: "dict | None") -> str:
    """Build a stable key string from a source dict for consultation tracking."""
    if not source:
        return "__global__"
    parts = []
    if source.get("session"):
        parts.append(f"session:{source['session']}")
    if source.get("agent"):
        parts.append(f"agent:{source['agent']}")
    if source.get("model"):
        parts.append(f"model:{source['model']}")
    return "|".join(parts) if parts else "__global__"


def record_schema_consultation(source: "dict | None", brick_name: str) -> None:
    """Record that get_brick_schema was called for brick_name from source.

    Thread/async safe enough for our use-case (GIL protects dict operations).
    """
    key = _source_key(source)
    import time as _t
    now = _t.time()
    if key not in _schema_consultations:
        _schema_consultations[key] = {}
    _schema_consultations[key][brick_name] = now


def was_schema_consulted(source: "dict | None", brick_name: str) -> bool:
    """Return True if get_brick_schema was previously called for brick_name from source.

    Entries older than _SCHEMA_CONSULTATION_TTL_SECONDS are treated as expired.
    """
    import time as _t
    key = _source_key(source)
    bucket = _schema_consultations.get(key)
    if not bucket:
        return False
    ts = bucket.get(brick_name)
    if ts is None:
        return False
    # Check TTL
    if _t.time() - ts > _SCHEMA_CONSULTATION_TTL_SECONDS:
        # Remove expired entry
        bucket.pop(brick_name, None)
        return False
    return True
