"""T-BRIX-DBO-20: Verify MCP tool schema descriptions are complete and informative.

All tool schemas must have proper descriptions (no TODO, no placeholder, min length).

Tests are split into:
- Seed-data tests: validate seed-data.json directly (no DB required)
- Seeded-DB tests: seed a temp DB from seed-data.json and verify the result
"""
import json
import sqlite3
from pathlib import Path

import pytest


SEED_FILE = Path(__file__).parent.parent / "seed-data.json"
MIN_DESCRIPTION_LENGTH = 20


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_seed_schemas() -> list[dict]:
    """Load mcp_tool_schemas from seed-data.json."""
    with SEED_FILE.open() as f:
        data = json.load(f)
    return data.get("mcp_tool_schemas", [])


def seed_tool_schemas_into_db(db) -> int:
    """Upsert all mcp_tool_schemas from seed-data.json into the given BrixDB."""
    schemas = load_seed_schemas()
    for s in schemas:
        db.mcp_tool_schemas_upsert({
            "name": s["name"],
            "description": s["description"],
            "input_schema": s.get("input_schema", {}),
        })
    return len(schemas)


# ---------------------------------------------------------------------------
# Seed-data tests (fast — no DB required)
# ---------------------------------------------------------------------------


class TestSeedDataDescriptions:
    """Validate descriptions in seed-data.json before they hit the DB."""

    def test_seed_file_exists(self):
        assert SEED_FILE.exists(), f"seed-data.json not found at {SEED_FILE}"

    def test_seed_has_minimum_tool_count(self):
        schemas = load_seed_schemas()
        assert len(schemas) >= 100, (
            f"Expected at least 100 tool schemas in seed-data.json, got {len(schemas)}"
        )

    def test_seed_no_short_descriptions(self):
        schemas = load_seed_schemas()
        violations = [
            s for s in schemas if len(s.get("description", "")) < MIN_DESCRIPTION_LENGTH
        ]
        if violations:
            details = "\n".join(
                f"  {s['name']} [{len(s.get('description',''))}]: {repr(s.get('description','')[:60])}"
                for s in violations
            )
            pytest.fail(
                f"{len(violations)} tool(s) with description < {MIN_DESCRIPTION_LENGTH} chars:\n{details}"
            )

    def test_seed_no_todo_descriptions(self):
        schemas = load_seed_schemas()
        violations = [s for s in schemas if "TODO" in s.get("description", "")]
        if violations:
            details = "\n".join(
                f"  {s['name']}: {repr(s.get('description','')[:80])}"
                for s in violations
            )
            pytest.fail(f"{len(violations)} tool(s) with 'TODO' in description:\n{details}")

    def test_seed_no_placeholder_descriptions(self):
        """'placeholder' as a technical term is allowed only in brix__instantiate_template."""
        schemas = load_seed_schemas()
        violations = [
            s for s in schemas
            if "placeholder" in s.get("description", "").lower()
            and s["name"] != "brix__instantiate_template"
        ]
        if violations:
            details = "\n".join(
                f"  {s['name']}: {repr(s.get('description','')[:80])}"
                for s in violations
            )
            pytest.fail(
                f"{len(violations)} tool(s) with unexpected 'placeholder' in description:\n{details}"
            )

    def test_seed_all_entries_have_name(self):
        schemas = load_seed_schemas()
        missing = [s for s in schemas if not s.get("name")]
        assert not missing, f"{len(missing)} tool schema entries without a name"

    def test_seed_descriptions_are_strings(self):
        schemas = load_seed_schemas()
        wrong_type = [
            s for s in schemas if not isinstance(s.get("description", ""), str)
        ]
        assert not wrong_type, (
            f"{len(wrong_type)} entries where description is not a string: "
            + ", ".join(s["name"] for s in wrong_type)
        )


# ---------------------------------------------------------------------------
# Seeded-DB tests (use the test-isolated DB populated from seed-data.json)
# ---------------------------------------------------------------------------


@pytest.fixture
def seeded_db(isolated_db):
    """Populate the isolated test DB with all tool schemas from seed-data.json."""
    count = seed_tool_schemas_into_db(isolated_db)
    assert count >= 100, f"Expected at least 100 schemas seeded, got {count}"
    return isolated_db


class TestSeededDBDescriptions:
    """Validate that tool schemas seeded from seed-data.json are correct in the DB."""

    def test_db_has_minimum_tool_count(self, seeded_db):
        schemas = seeded_db.mcp_tool_schemas_list()
        assert len(schemas) >= 100, (
            f"Expected at least 100 tool schemas in DB, got {len(schemas)}"
        )

    def test_db_no_short_descriptions(self, seeded_db):
        schemas = seeded_db.mcp_tool_schemas_list()
        violations = [
            s for s in schemas if len(s.get("description", "")) < MIN_DESCRIPTION_LENGTH
        ]
        if violations:
            details = "\n".join(
                f"  {s['name']} [{len(s.get('description',''))}]: {repr(s.get('description','')[:60])}"
                for s in violations
            )
            pytest.fail(
                f"{len(violations)} tool(s) with description < {MIN_DESCRIPTION_LENGTH} chars in DB:\n{details}"
            )

    def test_db_no_todo_descriptions(self, seeded_db):
        schemas = seeded_db.mcp_tool_schemas_list()
        violations = [s for s in schemas if "TODO" in s.get("description", "")]
        if violations:
            details = "\n".join(
                f"  {s['name']}: {repr(s.get('description','')[:80])}"
                for s in violations
            )
            pytest.fail(f"{len(violations)} tool(s) with 'TODO' in description in DB:\n{details}")

    def test_db_no_placeholder_descriptions(self, seeded_db):
        """'placeholder' as a technical term is allowed only in brix__instantiate_template."""
        schemas = seeded_db.mcp_tool_schemas_list()
        violations = [
            s for s in schemas
            if "placeholder" in s.get("description", "").lower()
            and s["name"] != "brix__instantiate_template"
        ]
        if violations:
            details = "\n".join(
                f"  {s['name']}: {repr(s.get('description','')[:80])}"
                for s in violations
            )
            pytest.fail(
                f"{len(violations)} tool(s) with unexpected 'placeholder' in DB description:\n{details}"
            )

    def test_db_seed_round_trip(self, seeded_db):
        """Descriptions round-trip: what we seeded from seed-data.json is what the DB returns."""
        seed_schemas = {s["name"]: s["description"] for s in load_seed_schemas()}
        db_rows = seeded_db.mcp_tool_schemas_list()
        db_schemas = {s["name"]: s.get("description", "") for s in db_rows}

        mismatches = []
        for name, seed_desc in seed_schemas.items():
            db_desc = db_schemas.get(name)
            if db_desc is None:
                mismatches.append(f"  MISSING in DB: {name}")
            elif db_desc != seed_desc:
                mismatches.append(
                    f"  MISMATCH {name}:\n"
                    f"    seed [{len(seed_desc)}]: {repr(seed_desc[:60])}\n"
                    f"    db   [{len(db_desc)}]: {repr(db_desc[:60])}"
                )

        if mismatches:
            pytest.fail(
                f"{len(mismatches)} seed/DB description mismatches:\n"
                + "\n".join(mismatches)
            )
