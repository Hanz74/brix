"""Export seeded DB tables to seed-data.json for code-independent seeding."""
from __future__ import annotations

import json
import logging
from pathlib import Path

from brix.db import BrixDB

logger = logging.getLogger(__name__)

# Fields to strip before export (DB-internal timestamps etc.)
_STRIP_FIELDS = {"created_at", "updated_at"}

# Fields stored as JSON strings in DB that should be decoded for clean export.
# Re-importing via brick_definitions_upsert will re-serialize them.
_BRICK_JSON_FIELDS = {"config_schema", "aliases", "examples"}
_CONNECTOR_JSON_FIELDS = {"required_mcp_tools", "output_schema", "parameters", "related_pipelines", "related_helpers"}
_TOOL_JSON_FIELDS = {"input_schema"}


def _clean(record: dict, json_fields: set[str] | None = None) -> dict:
    """Remove internal DB fields and decode JSON-string fields from a record.

    Args:
        record: Raw DB row dict.
        json_fields: Set of field names that are stored as JSON strings in the
                     DB and should be decoded back to Python objects for export.
    """
    result = {}
    for k, v in record.items():
        if k in _STRIP_FIELDS:
            continue
        # Decode JSON-encoded fields so they round-trip correctly through upsert
        if json_fields and k in json_fields and isinstance(v, str):
            try:
                v = json.loads(v)
            except (json.JSONDecodeError, TypeError):
                pass
        result[k] = v
    return result


def export_seed_data(output_path: str = "seed-data.json") -> None:
    """Export all seeded DB tables to a JSON file.

    Args:
        output_path: Destination file path (absolute or relative).
    """
    db = BrixDB()

    data: dict[str, list[dict]] = {
        "brick_definitions": [_clean(r, _BRICK_JSON_FIELDS) for r in db.brick_definitions_list()],
        "connector_definitions": [_clean(r, _CONNECTOR_JSON_FIELDS) for r in db.connector_definitions_list()],
        "mcp_tool_schemas": [_clean(r, _TOOL_JSON_FIELDS) for r in db.mcp_tool_schemas_list()],
        "help_topics": [_clean(r) for r in db.help_topics_list()],
        "keyword_taxonomies": [_clean(r) for r in db.keyword_taxonomies_list()],
        "type_compatibility": [_clean(r) for r in db.type_compatibility_list()],
    }

    out = Path(output_path)
    out.parent.mkdir(parents=True, exist_ok=True)

    with open(out, "w", encoding="utf-8") as f:
        json.dump(data, f, indent=2, ensure_ascii=False)

    total = sum(len(v) for v in data.values())
    counts = {k: len(v) for k, v in data.items()}
    logger.info("Exported %d entries to %s: %s", total, output_path, counts)
    print(f"Exported {total} entries to {output_path}")
    for table, count in counts.items():
        print(f"  {table}: {count}")


if __name__ == "__main__":
    import sys

    path = sys.argv[1] if len(sys.argv) > 1 else "seed-data.json"
    export_seed_data(path)
