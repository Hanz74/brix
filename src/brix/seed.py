"""DB-First seeding — T-BRIX-DB-06 / T-BRIX-DB-08.

Fills DB tables from seed-data.json on first start (code-independent).
Falls back to code imports if seed-data.json is not found (transition phase).
Called ONLY when tables are empty (once per fresh DB).
After seeding, neither seed-data.json nor code definitions are read at runtime.
"""
from __future__ import annotations

import json
import logging
from pathlib import Path

from brix.db import BrixDB

logger = logging.getLogger(__name__)

# seed-data.json lives in the project root (two levels above this file)
_SEED_FILE = Path(__file__).parent.parent.parent / "seed-data.json"


def seed_if_empty(db: BrixDB) -> dict[str, int]:
    """Seed all DB-First tables if they are empty.

    Reads exclusively from seed-data.json. Raises FileNotFoundError when the
    seed file is missing — no silent fallback to code imports.

    Returns a dict with the count of rows seeded per table.
    Skips any table that already has data (idempotent).
    """
    if not _SEED_FILE.exists():
        raise FileNotFoundError(
            f"seed-data.json not found at {_SEED_FILE}. "
            "Cannot seed the database without the seed file. "
            "Run 'python -m brix.export_seed_data' to regenerate it."
        )

    logger.debug("seed_if_empty: reading from %s", _SEED_FILE)
    counts = _seed_from_file(db, _SEED_FILE)

    counts["system_pipelines"] = _seed_system_pipelines()

    # DB-First content import: pipelines and helpers from filesystem into DB
    counts["pipeline_content_imported"] = import_pipeline_content(db)
    counts["helper_code_imported"] = import_helper_code(db)

    # Migrate legacy step types in all DB pipelines
    counts["legacy_steps_migrated"] = migrate_legacy_step_types(db)

    return counts


def _seed_system_pipelines() -> int:
    """Seed system pipelines into the default pipeline store if not already present."""
    try:
        from brix.system_pipelines import seed_system_pipelines
        from brix.pipeline_store import PipelineStore
    except ImportError as e:
        logger.warning("Cannot seed system pipelines: %s", e)
        return 0

    store = PipelineStore()
    return seed_system_pipelines(store)


# ---------------------------------------------------------------------------
# JSON-file based seeding (primary path — T-BRIX-DB-08)
# ---------------------------------------------------------------------------

def _seed_from_file(db: BrixDB, seed_file: Path) -> dict[str, int]:
    """Read seed-data.json and seed each table if empty."""
    with open(seed_file, encoding="utf-8") as f:
        data: dict[str, list[dict]] = json.load(f)

    counts: dict[str, int] = {}
    counts["brick_definitions"] = _seed_bricks_from_data(db, data.get("brick_definitions", []))
    counts["connector_definitions"] = _seed_connectors_from_data(db, data.get("connector_definitions", []))
    counts["mcp_tool_schemas"] = _seed_tools_from_data(db, data.get("mcp_tool_schemas", []))
    counts["help_topics"] = _seed_help_from_data(db, data.get("help_topics", []))
    counts["keyword_taxonomies"] = _seed_keywords_from_data(db, data.get("keyword_taxonomies", []))
    counts["type_compatibility"] = _seed_types_from_data(db, data.get("type_compatibility", []))

    total = sum(counts.values())
    if total > 0:
        logger.info("DB-First seed (file): %d rows seeded across %d tables", total, len(counts))

    return counts


def _seed_bricks_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.brick_definitions_count() > 0:
        return 0
    for rec in records:
        db.brick_definitions_upsert(rec)
    logger.debug("Seeded %d brick_definitions", len(records))
    return len(records)


def _seed_connectors_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.connector_definitions_count() > 0:
        return 0
    for rec in records:
        db.connector_definitions_upsert(rec)
    logger.debug("Seeded %d connector_definitions", len(records))
    return len(records)


def _seed_tools_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.mcp_tool_schemas_count() > 0:
        return 0
    for rec in records:
        db.mcp_tool_schemas_upsert(rec)
    logger.debug("Seeded %d mcp_tool_schemas", len(records))
    return len(records)


def _seed_help_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.help_topics_count() > 0:
        return 0
    for rec in records:
        db.help_topics_upsert(rec)
    logger.debug("Seeded %d help_topics", len(records))
    return len(records)


def _seed_keywords_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.keyword_taxonomies_count() > 0:
        return 0
    for rec in records:
        db.keyword_taxonomies_upsert(
            category=rec["category"],
            keyword=rec["keyword"],
            language=rec.get("language", "de"),
            mapped_to=rec.get("mapped_to", ""),
        )
    logger.debug("Seeded %d keyword_taxonomies", len(records))
    return len(records)


def _seed_types_from_data(db: BrixDB, records: list[dict]) -> int:
    if db.type_compatibility_count() > 0:
        return 0
    for rec in records:
        db.type_compatibility_upsert(
            output_type=rec["output_type"],
            compatible_input=rec["compatible_input"],
        )
    logger.debug("Seeded %d type_compatibility rows", len(records))
    return len(records)


# ---------------------------------------------------------------------------
# Code-import based seeding (fallback / legacy path — T-BRIX-DB-06)
# ---------------------------------------------------------------------------

def _seed_from_code(db: BrixDB) -> dict[str, int]:
    """Seed from Python code definitions (legacy fallback)."""
    counts: dict[str, int] = {}

    counts["brick_definitions"] = _seed_brick_definitions(db)
    counts["connector_definitions"] = _seed_connector_definitions(db)
    counts["mcp_tool_schemas"] = _seed_mcp_tool_schemas(db)
    counts["help_topics"] = _seed_help_topics(db)
    counts["keyword_taxonomies"] = _seed_keyword_taxonomies(db)
    counts["type_compatibility"] = _seed_type_compatibility(db)

    total = sum(counts.values())
    if total > 0:
        logger.info("DB-First seed (code): %d rows seeded across %d tables", total, len(counts))

    return counts


def _seed_brick_definitions(db: BrixDB) -> int:
    """Seed brick_definitions from ALL_BUILTINS if table is empty."""
    if db.brick_definitions_count() > 0:
        return 0

    try:
        from brix.bricks.builtins import ALL_BUILTINS
    except ImportError as e:
        logger.warning("Cannot seed brick_definitions: %s", e)
        return 0

    count = 0
    for brick in ALL_BUILTINS:
        # Serialise config_schema dict[str, BrickParam] → plain dict
        config_schema_dict: dict = {}
        for param_name, param in (brick.config_schema or {}).items():
            config_schema_dict[param_name] = {
                "type": param.type,
                "description": param.description,
                "default": param.default,
                "required": param.required,
                "enum": param.enum,
            }

        db.brick_definitions_upsert({
            "name": brick.name,
            "runner": brick.runner or brick.type,
            "namespace": brick.namespace or "",
            "category": brick.category or "",
            "description": brick.description or "",
            "when_to_use": brick.when_to_use or "",
            "when_NOT_to_use": brick.when_NOT_to_use or "",
            "aliases": list(brick.aliases or []),
            "input_type": brick.input_type or "*",
            "output_type": brick.output_type or "*",
            "config_schema": config_schema_dict,
            "examples": list(brick.examples or []),
            "related_connector": brick.related_connector or "",
            "system": bool(getattr(brick, "system", False)),
        })
        count += 1

    logger.debug("Seeded %d brick_definitions", count)
    return count


def _seed_connector_definitions(db: BrixDB) -> int:
    """Seed connector_definitions from CONNECTOR_REGISTRY if table is empty."""
    if db.connector_definitions_count() > 0:
        return 0

    try:
        from brix.connectors import CONNECTOR_REGISTRY
    except ImportError as e:
        logger.warning("Cannot seed connector_definitions: %s", e)
        return 0

    count = 0
    for connector in CONNECTOR_REGISTRY.values():
        # Serialise parameters list[ConnectorParam] → plain list[dict]
        params_list = [
            {
                "name": p.name,
                "type": p.type,
                "description": p.description,
                "required": p.required,
                "default": p.default,
            }
            for p in (connector.parameters or [])
        ]

        db.connector_definitions_upsert({
            "name": connector.name,
            "type": connector.type,
            "description": connector.description or "",
            "required_mcp_server": connector.required_mcp_server or "",
            "required_mcp_tools": list(connector.required_mcp_tools or []),
            "output_schema": dict(connector.output_schema or {}),
            "parameters": params_list,
            "related_pipelines": list(connector.related_pipelines or []),
            "related_helpers": list(connector.related_helpers or []),
        })
        count += 1

    logger.debug("Seeded %d connector_definitions", count)
    return count


def _seed_mcp_tool_schemas(db: BrixDB) -> int:
    """Seed mcp_tool_schemas from BRIX_TOOLS if table is empty (legacy fallback)."""
    if db.mcp_tool_schemas_count() > 0:
        return 0

    try:
        from brix.mcp_tools_schema import BRIX_TOOLS  # noqa: F401
    except ImportError:
        logger.warning("Cannot seed mcp_tool_schemas: mcp_tools_schema.py moved to backup (DB-First)")
        return 0

    count = 0
    for tool in BRIX_TOOLS:
        db.mcp_tool_schemas_upsert({
            "name": tool.name,
            "description": tool.description or "",
            "input_schema": dict(tool.inputSchema) if tool.inputSchema else {},
        })
        count += 1

    logger.debug("Seeded %d mcp_tool_schemas", count)
    return count


def _seed_help_topics(db: BrixDB) -> int:
    """Seed help_topics from _HELP_TOPICS if table is empty (legacy fallback)."""
    if db.help_topics_count() > 0:
        return 0

    try:
        from brix.mcp_help_content import _HELP_TOPICS, _HELP_TOPIC_DESCRIPTIONS  # noqa: F401
    except ImportError:
        logger.warning("Cannot seed help_topics: mcp_help_content.py moved to backup (DB-First)")
        return 0

    count = 0
    for name, content in _HELP_TOPICS.items():
        db.help_topics_upsert({
            "name": name,
            "title": _HELP_TOPIC_DESCRIPTIONS.get(name, name),
            "content": content,
        })
        count += 1

    logger.debug("Seeded %d help_topics", count)
    return count


def _seed_keyword_taxonomies(db: BrixDB) -> int:
    """Seed keyword_taxonomies from composer _*_KEYWORDS dicts if table is empty."""
    if db.keyword_taxonomies_count() > 0:
        return 0

    try:
        from brix.mcp_handlers.composer import (
            _SOURCE_KEYWORDS,
            _ACTION_KEYWORDS,
            _TARGET_KEYWORDS,
        )
    except ImportError as e:
        logger.warning("Cannot seed keyword_taxonomies: %s", e)
        return 0

    count = 0
    for mapped_to, keywords in _SOURCE_KEYWORDS.items():
        for kw in keywords:
            db.keyword_taxonomies_upsert("source", kw, "de", mapped_to)
            count += 1

    for mapped_to, keywords in _ACTION_KEYWORDS.items():
        for kw in keywords:
            db.keyword_taxonomies_upsert("action", kw, "de", mapped_to)
            count += 1

    for mapped_to, keywords in _TARGET_KEYWORDS.items():
        for kw in keywords:
            db.keyword_taxonomies_upsert("target", kw, "de", mapped_to)
            count += 1

    logger.debug("Seeded %d keyword_taxonomies", count)
    return count


def _seed_type_compatibility(db: BrixDB) -> int:
    """Seed type_compatibility from TYPE_COMPATIBILITY dict if table is empty."""
    if db.type_compatibility_count() > 0:
        return 0

    try:
        from brix.bricks.types import TYPE_COMPATIBILITY
    except ImportError as e:
        logger.warning("Cannot seed type_compatibility: %s", e)
        return 0

    count = 0
    for output_type, compatible_inputs in TYPE_COMPATIBILITY.items():
        for compatible_input in compatible_inputs:
            db.type_compatibility_upsert(output_type, compatible_input)
            count += 1

    logger.debug("Seeded %d type_compatibility rows", count)
    return count


# ---------------------------------------------------------------------------
# DB-First content import — pipelines YAML and helper code into DB
# ---------------------------------------------------------------------------

_PIPELINE_SEARCH_PATHS = [
    Path.home() / ".brix" / "pipelines",
    Path("/app/pipelines"),
]

_HELPER_SEARCH_PATHS = [
    Path.home() / ".brix" / "helpers",
    Path("/app/helpers"),
]


# Pipeline name prefixes that indicate test/development artifacts — never import these.
_TEST_PIPELINE_PREFIXES = (
    "test", "xtest", "pipe_", "uuid_", "assert", "mock", "fail", "compat",
    "desc_", "listed_", "exposed", "my_", "no_", "same_", "tracked", "upd_",
    "update_", "rmstep", "step_", "to_delete",
)


def _is_test_pipeline(name: str) -> bool:
    """Return True if a pipeline name looks like a test/development artifact."""
    name_lower = name.lower()
    return any(name_lower.startswith(prefix) for prefix in _TEST_PIPELINE_PREFIXES)


def import_pipeline_content(db: BrixDB) -> int:
    """Import pipeline YAML content from filesystem into DB.

    Only imports if the DB has pipelines without yaml_content.
    Idempotent: skips pipelines that already have content.
    Test-pipeline names (see _TEST_PIPELINE_PREFIXES) are never imported.
    """
    import yaml as _yaml

    if db.count_pipelines_with_content() > 0:
        # Already imported — skip
        return 0

    count = 0
    seen: set[str] = set()

    for search_dir in _PIPELINE_SEARCH_PATHS:
        if not search_dir.exists():
            continue
        for ext in ("*.yaml", "*.yml"):
            for f in sorted(search_dir.glob(ext)):
                name = f.stem
                if name in seen:
                    continue
                seen.add(name)
                if _is_test_pipeline(name):
                    logger.debug("Skipping test pipeline '%s' during seed import", name)
                    continue
                try:
                    content = f.read_text(encoding="utf-8")
                    data = _yaml.safe_load(content) or {}
                    requirements = data.get("requirements", [])
                    if not isinstance(requirements, list):
                        requirements = []
                    db.upsert_pipeline(
                        name=name,
                        path=str(f),
                        requirements=requirements,
                        yaml_content=content,
                    )
                    count += 1
                except Exception as exc:
                    logger.warning("Failed to import pipeline %s: %s", name, exc)

    if count > 0:
        logger.info("Imported %d pipeline YAML files into DB", count)
    return count


def import_helper_code(db: BrixDB) -> int:
    """Import helper Python code from filesystem into DB.

    Only imports if the DB has helpers without code.
    Idempotent: skips helpers that already have code.
    """
    if db.count_helpers_with_code() > 0:
        # Already imported — skip
        return 0

    count = 0
    seen: set[str] = set()

    for search_dir in _HELPER_SEARCH_PATHS:
        if not search_dir.exists():
            continue
        for f in sorted(search_dir.glob("*.py")):
            name = f.stem
            if name in seen:
                continue
            if name.startswith("__"):
                continue  # skip __init__.py etc.
            seen.add(name)
            try:
                code = f.read_text(encoding="utf-8")
                # Get existing helper info from DB or create new entry
                existing = db.get_helper(name)
                db.upsert_helper(
                    name=name,
                    script_path=str(f),
                    description=existing.get("description", "") if existing else "",
                    requirements=existing.get("requirements", []) if existing else [],
                    input_schema=existing.get("input_schema", {}) if existing else {},
                    output_schema=existing.get("output_schema", {}) if existing else {},
                    helper_id=existing.get("id") if existing else None,
                    code=code,
                )
                count += 1
            except Exception as exc:
                logger.warning("Failed to import helper %s: %s", name, exc)

    if count > 0:
        logger.info("Imported %d helper scripts into DB", count)
    return count


# ---------------------------------------------------------------------------
# Auto-tagging by prefix — T-BRIX-ORG-01
# ---------------------------------------------------------------------------

# Maps pipeline/helper name prefix → project label.
# First match wins (evaluated top-to-bottom).
_PREFIX_TO_PROJECT: list[tuple[str, str]] = [
    ("buddy-", "buddy"),
    ("buddy_", "buddy"),
    ("cody-", "cody"),
    ("cody_", "cody"),
    ("_system/", "system"),
    ("_system-", "system"),
    # Utility-action prefixes
    ("download-", "utility"),
    ("download_", "utility"),
    ("convert-", "utility"),
    ("convert_", "utility"),
    ("import-", "utility"),
    ("import_", "utility"),
    ("analyze-", "utility"),
    ("analyze_", "utility"),
    ("generate-", "utility"),
    ("generate_", "utility"),
    ("enrich-", "utility"),
    ("enrich_", "utility"),
    ("apply-", "utility"),
    ("apply_", "utility"),
    # Test prefixes
    ("test-", "test"),
    ("test_", "test"),
    ("xtest-", "test"),
    ("xtest_", "test"),
    ("assert-", "test"),
    ("assert_", "test"),
    ("mock-", "test"),
    ("mock_", "test"),
    ("fail-", "test"),
    ("fail_", "test"),
]


def _infer_project_from_name(name: str) -> str:
    """Return the project label for a pipeline/helper name based on prefix rules."""
    name_lower = name.lower()
    for prefix, project in _PREFIX_TO_PROJECT:
        if name_lower.startswith(prefix):
            return project
    return ""


def auto_tag_by_prefix(db: BrixDB) -> dict[str, int]:
    """Apply project labels to all pipelines and helpers that have no project set yet.

    Pipelines/helpers that already have a non-empty project are left unchanged.
    Returns dict with counts: {pipelines_tagged, helpers_tagged}.
    """
    counts: dict[str, int] = {"pipelines_tagged": 0, "helpers_tagged": 0}

    # Tag pipelines
    for p in db.list_pipelines():
        if p.get("project"):
            continue  # already tagged
        inferred = _infer_project_from_name(p["name"])
        if inferred:
            db.pipeline_set_project(p["name"], inferred)
            counts["pipelines_tagged"] += 1
            logger.debug("auto_tag: pipeline '%s' → project '%s'", p["name"], inferred)

    # Tag helpers
    for h in db.list_helpers():
        if h.get("project"):
            continue  # already tagged
        inferred = _infer_project_from_name(h["name"])
        if inferred:
            db.helper_set_project(h["name"], inferred)
            counts["helpers_tagged"] += 1
            logger.debug("auto_tag: helper '%s' → project '%s'", h["name"], inferred)

    total = counts["pipelines_tagged"] + counts["helpers_tagged"]
    if total > 0:
        logger.info("auto_tag_by_prefix: tagged %d items total", total)
    return counts


def delete_test_pipelines(db: BrixDB) -> int:
    """Delete all pipelines that have project='test'.

    Returns the number of pipelines deleted.
    """
    deleted = db.delete_pipelines_by_project("test")
    if deleted > 0:
        logger.info("delete_test_pipelines: removed %d test pipelines", deleted)
    return deleted


# ---------------------------------------------------------------------------
# Legacy step-type migration
# ---------------------------------------------------------------------------

LEGACY_STEP_TYPE_MAP = {
    "python": "script.python",
    "http": "http.request",
    "mcp": "mcp.call",
    "cli": "script.cli",
    "filter": "flow.filter",
    "transform": "flow.transform",
    "set": "flow.set",
    "repeat": "flow.repeat",
    "choose": "flow.choose",
    "parallel": "flow.parallel",
    "pipeline": "flow.pipeline",
    "pipeline_group": "flow.pipeline_group",
    "validate": "flow.validate",
    "notify": "action.notify",
    "approval": "action.approval",
    "specialist": "extract.specialist",
}


def _migrate_steps_in_list(steps: list, changed: list) -> None:
    """Recursively migrate legacy step types in a step list. Mutates in-place."""
    for step in steps:
        if not isinstance(step, dict):
            continue
        step_type = step.get("type", "")
        if step_type in LEGACY_STEP_TYPE_MAP:
            step["type"] = LEGACY_STEP_TYPE_MAP[step_type]
            changed.append(step.get("id", "unknown"))
        # Recurse into nested structures
        if "sequence" in step and isinstance(step["sequence"], list):
            _migrate_steps_in_list(step["sequence"], changed)
        if "choices" in step and isinstance(step["choices"], list):
            for choice in step["choices"]:
                if isinstance(choice, dict) and "steps" in choice:
                    _migrate_steps_in_list(choice["steps"], changed)
        if "default_steps" in step and isinstance(step["default_steps"], list):
            _migrate_steps_in_list(step["default_steps"], changed)
        if "sub_steps" in step and isinstance(step["sub_steps"], list):
            _migrate_steps_in_list(step["sub_steps"], changed)


def migrate_legacy_step_types(db: BrixDB) -> int:
    """Migrate all legacy step types in DB-stored pipeline YAML content.

    Reads yaml_content from all pipelines, replaces old type names with
    namespaced equivalents, and writes back to DB. Idempotent.
    Returns the number of pipelines modified.
    """
    import yaml as _yaml

    pipelines = db.list_pipelines()
    modified_count = 0

    for p in pipelines:
        yaml_content = db.get_pipeline_yaml_content(p["name"])
        if not yaml_content:
            continue

        try:
            data = _yaml.safe_load(yaml_content)
            if not isinstance(data, dict):
                continue
        except Exception:
            continue

        steps = data.get("steps", [])
        if not isinstance(steps, list):
            continue

        changed: list = []
        _migrate_steps_in_list(steps, changed)

        if changed:
            new_content = _yaml.dump(data, default_flow_style=False, allow_unicode=True)
            db.upsert_pipeline(
                name=p["name"],
                path=p.get("path", ""),
                requirements=p.get("requirements", []),
                yaml_content=new_content,
            )
            modified_count += 1
            logger.debug(
                "Migrated %d legacy step types in pipeline '%s': %s",
                len(changed), p["name"], changed
            )

    if modified_count > 0:
        logger.info("Migrated legacy step types in %d pipelines", modified_count)
    return modified_count
