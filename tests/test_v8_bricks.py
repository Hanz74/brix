"""Tests for T-BRIX-V8-05: 12 atomic domain bricks."""
import pytest

from brix.bricks.builtins import ALL_BUILTINS
from brix.bricks.registry import BrickRegistry

# Names of the 12 new bricks
NEW_BRICK_NAMES = [
    "source.fetch_emails",
    "source.fetch_files",
    "source.http_fetch",
    "convert.to_markdown",
    "convert.to_json",
    "convert.extract_text",
    "llm.extract",
    "llm.classify",
    "db.ingest",
    "db.query",
    "action.notify",
    "action.move_file",
]

REQUIRED_FIELDS = [
    "name",
    "type",
    "description",
    "when_to_use",
    "when_NOT_to_use",
    "category",
    "aliases",
    "input_type",
    "output_type",
    "config_schema",
    "examples",
]


@pytest.fixture
def registry():
    return BrickRegistry()


# ---------------------------------------------------------------------------
# Presence tests
# ---------------------------------------------------------------------------

def test_all_12_bricks_in_all_builtins():
    """All 12 new atomic bricks must be registered in ALL_BUILTINS."""
    builtin_names = {b.name for b in ALL_BUILTINS}
    for name in NEW_BRICK_NAMES:
        assert name in builtin_names, f"Brick '{name}' not found in ALL_BUILTINS"


def test_all_12_bricks_in_registry(registry):
    """All 12 bricks must be discoverable via the registry."""
    for name in NEW_BRICK_NAMES:
        brick = registry.get(name)
        assert brick is not None, f"Brick '{name}' not found in registry"


# ---------------------------------------------------------------------------
# Required fields tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", NEW_BRICK_NAMES)
def test_brick_has_all_required_fields(registry, name):
    """Each brick must have all required fields with non-empty values."""
    brick = registry.get(name)
    assert brick is not None, f"Brick '{name}' not found"

    assert brick.name, "name must be non-empty"
    assert brick.type, "type must be non-empty"
    assert brick.description, "description must be non-empty"
    assert brick.when_to_use, "when_to_use must be non-empty"
    assert brick.when_NOT_to_use, "when_NOT_to_use must be non-empty"
    assert brick.category, "category must be non-empty"
    assert isinstance(brick.aliases, list), "aliases must be a list"
    assert len(brick.aliases) > 0, "aliases must not be empty"
    assert brick.input_type, "input_type must be non-empty"
    assert brick.output_type, "output_type must be non-empty"
    assert isinstance(brick.config_schema, dict), "config_schema must be a dict"
    assert isinstance(brick.examples, list), "examples must be a list"
    assert len(brick.examples) > 0, "examples must have at least one entry"


@pytest.mark.parametrize("name", NEW_BRICK_NAMES)
def test_brick_when_not_to_use_present(registry, name):
    """when_NOT_to_use must be explicitly set for every new brick."""
    brick = registry.get(name)
    assert brick is not None
    assert brick.when_NOT_to_use, f"Brick '{name}' is missing when_NOT_to_use"
    assert len(brick.when_NOT_to_use) >= 20, (
        f"Brick '{name}' when_NOT_to_use is too short (< 20 chars): {brick.when_NOT_to_use!r}"
    )


# ---------------------------------------------------------------------------
# Category tests
# ---------------------------------------------------------------------------

def test_categories_correct(registry):
    """Verify category distribution: source(3), convert(3), llm(2), db(2), action(2)."""
    category_counts: dict[str, int] = {}
    for name in NEW_BRICK_NAMES:
        brick = registry.get(name)
        assert brick is not None
        category_counts[brick.category] = category_counts.get(brick.category, 0) + 1

    assert category_counts.get("source", 0) == 3, f"Expected 3 source bricks, got {category_counts}"
    assert category_counts.get("convert", 0) == 3, f"Expected 3 convert bricks, got {category_counts}"
    assert category_counts.get("llm", 0) == 2, f"Expected 2 llm bricks, got {category_counts}"
    assert category_counts.get("db", 0) == 2, f"Expected 2 db bricks, got {category_counts}"
    assert category_counts.get("action", 0) == 2, f"Expected 2 action bricks, got {category_counts}"


def test_source_bricks(registry):
    """source.* bricks must be in category 'source'."""
    for name in ["source.fetch_emails", "source.fetch_files", "source.http_fetch"]:
        brick = registry.get(name)
        assert brick.category == "source", f"{name} should be in category 'source', got '{brick.category}'"


def test_convert_bricks(registry):
    """convert.* bricks must be in category 'convert'."""
    for name in ["convert.to_markdown", "convert.to_json", "convert.extract_text"]:
        brick = registry.get(name)
        assert brick.category == "convert", f"{name} should be in category 'convert', got '{brick.category}'"


def test_llm_bricks(registry):
    """llm.* bricks must be in category 'llm'."""
    for name in ["llm.extract", "llm.classify"]:
        brick = registry.get(name)
        assert brick.category == "llm", f"{name} should be in category 'llm', got '{brick.category}'"


def test_db_bricks(registry):
    """db.* bricks must be in category 'db'."""
    for name in ["db.ingest", "db.query"]:
        brick = registry.get(name)
        assert brick.category == "db", f"{name} should be in category 'db', got '{brick.category}'"


def test_action_bricks(registry):
    """action.* bricks must be in category 'action'."""
    for name in ["action.notify", "action.move_file"]:
        brick = registry.get(name)
        assert brick.category == "action", f"{name} should be in category 'action', got '{brick.category}'"


# ---------------------------------------------------------------------------
# Search / alias tests
# ---------------------------------------------------------------------------

def test_search_by_english_alias(registry):
    """search_bricks must find bricks via English aliases."""
    results = registry.search("fetch emails")
    names = {b.name for b in results}
    assert "source.fetch_emails" in names, "search('fetch emails') should find source.fetch_emails"


def test_search_by_german_alias(registry):
    """search_bricks must find bricks via German aliases."""
    results = registry.search("mails abrufen")
    names = {b.name for b in results}
    assert "source.fetch_emails" in names, "search('mails abrufen') should find source.fetch_emails"


def test_search_markitdown(registry):
    """Searching for 'markitdown' should find convert.to_markdown."""
    results = registry.search("markitdown")
    names = {b.name for b in results}
    assert "convert.to_markdown" in names


def test_search_kategorisieren(registry):
    """Searching for German 'kategorisieren' should find llm.classify."""
    results = registry.search("kategorisieren")
    names = {b.name for b in results}
    assert "llm.classify" in names


def test_search_ocr(registry):
    """Searching for 'ocr' should find convert.extract_text."""
    results = registry.search("ocr")
    names = {b.name for b in results}
    assert "convert.extract_text" in names


def test_search_datenbank_schreiben(registry):
    """Searching for 'datenbank schreiben' should find db.ingest."""
    results = registry.search("datenbank schreiben")
    names = {b.name for b in results}
    assert "db.ingest" in names


def test_search_benachrichtigung(registry):
    """Searching for 'benachrichtigung' should find action.notify."""
    results = registry.search("benachrichtigung")
    names = {b.name for b in results}
    assert "action.notify" in names


def test_search_datei_verschieben(registry):
    """Searching for 'datei verschieben' should find action.move_file."""
    results = registry.search("datei verschieben")
    names = {b.name for b in results}
    assert "action.move_file" in names


def test_search_by_category_filter(registry):
    """list_by_category must return only bricks of the given category."""
    source_bricks = registry.list_by_category("source")
    source_names = {b.name for b in source_bricks}
    assert "source.fetch_emails" in source_names
    assert "source.fetch_files" in source_names
    assert "source.http_fetch" in source_names
    # No non-source bricks
    for name in source_names:
        brick = registry.get(name)
        assert brick.category == "source"


# ---------------------------------------------------------------------------
# get_brick_schema tests
# ---------------------------------------------------------------------------

def test_get_brick_schema_source_fetch_emails(registry):
    """get brick schema returns correct schema for source.fetch_emails."""
    brick = registry.get("source.fetch_emails")
    assert brick is not None
    schema = brick.to_json_schema()
    assert schema["type"] == "object"
    assert "provider" in schema["properties"]
    assert "provider" in schema.get("required", [])


def test_get_brick_schema_llm_extract(registry):
    """get brick schema returns correct schema for llm.extract."""
    brick = registry.get("llm.extract")
    schema = brick.to_json_schema()
    assert "prompt_template" in schema["properties"]
    assert "output_schema" in schema["properties"]
    assert "model" in schema["properties"]
    assert "prompt_template" in schema.get("required", [])
    assert "output_schema" in schema.get("required", [])


def test_get_brick_schema_db_ingest(registry):
    """get brick schema returns correct schema for db.ingest."""
    brick = registry.get("db.ingest")
    schema = brick.to_json_schema()
    assert "table" in schema["properties"]
    assert "table" in schema.get("required", [])


def test_get_brick_schema_action_move_file(registry):
    """get brick schema returns correct schema for action.move_file."""
    brick = registry.get("action.move_file")
    schema = brick.to_json_schema()
    assert "source" in schema["properties"]
    assert "destination" in schema["properties"]
    assert "operation" in schema["properties"]
    required = schema.get("required", [])
    assert "source" in required
    assert "destination" in required


# ---------------------------------------------------------------------------
# Examples tests
# ---------------------------------------------------------------------------

@pytest.mark.parametrize("name", NEW_BRICK_NAMES)
def test_examples_have_goal_and_config(registry, name):
    """Each example in a brick must have 'goal' and 'config' keys."""
    brick = registry.get(name)
    for i, example in enumerate(brick.examples):
        assert "goal" in example, f"{name} example[{i}] missing 'goal'"
        assert "config" in example, f"{name} example[{i}] missing 'config'"
        assert example["goal"], f"{name} example[{i}] 'goal' is empty"
        assert isinstance(example["config"], dict), f"{name} example[{i}] 'config' must be dict"


# ---------------------------------------------------------------------------
# Total count test
# ---------------------------------------------------------------------------

def test_total_new_bricks_count():
    """Exactly 12 new atomic domain bricks must exist in ALL_BUILTINS."""
    builtin_names = {b.name for b in ALL_BUILTINS}
    found = [name for name in NEW_BRICK_NAMES if name in builtin_names]
    assert len(found) == 12, f"Expected 12 new bricks, found {len(found)}: {found}"


def test_all_builtins_has_at_least_23_entries():
    """ALL_BUILTINS must have at least 23 entries (11 original + 12 new)."""
    assert len(ALL_BUILTINS) >= 23, f"Expected at least 23 bricks, got {len(ALL_BUILTINS)}"
