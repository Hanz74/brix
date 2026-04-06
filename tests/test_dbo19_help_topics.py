"""
T-BRIX-DBO-19 — Help Topic Quality Tests

Verifies:
  1. All help topics have non-empty content
  2. No help topic content contains the deprecated 'yaml_content' field reference
  3. 'org-fields' topic exists and documents project, tags, and group
"""
import sqlite3
from pathlib import Path

import pytest

DB_PATH = Path(__file__).parent.parent / "tests" / "fixtures" / "help_topics_test.db"


def get_db_path() -> Path:
    """Return path to the live brix.db inside the container mount or the host fallback."""
    # Running inside container
    container_db = Path("/root/.brix/brix.db")
    if container_db.exists():
        return container_db
    # Running on host (e.g. via pytest directly in /root/docker/brix)
    host_db = Path(__file__).parent.parent / "brix.db"
    if host_db.exists():
        return host_db
    pytest.skip("brix.db not found — run tests inside container or mount DB first")


@pytest.fixture(scope="module")
def help_topics():
    """Load all help topics from the DB."""
    db_path = get_db_path()
    conn = sqlite3.connect(str(db_path))
    conn.row_factory = sqlite3.Row
    try:
        rows = conn.execute("SELECT name, title, content, category FROM help_topic ORDER BY name").fetchall()
    finally:
        conn.close()
    return [dict(row) for row in rows]


def test_help_topics_exist(help_topics):
    """At least some help topics must exist in the DB."""
    assert len(help_topics) >= 5, f"Expected at least 5 help topics, got {len(help_topics)}"


def test_all_help_topics_have_non_empty_content(help_topics):
    """Every help topic must have a non-empty content field."""
    empty = [t["name"] for t in help_topics if not (t.get("content") or "").strip()]
    assert empty == [], f"Help topics with empty content: {empty}"


def test_no_yaml_content_field_references(help_topics):
    """No help topic content should reference the deprecated 'yaml_content' DB field.

    yaml_content is a deprecated backup column. Help docs must not instruct users
    to read or write it — DB-first CRUD tools are the only interface.
    """
    stale = [t["name"] for t in help_topics if "yaml_content" in (t.get("content") or "")]
    assert stale == [], (
        f"Help topics still referencing deprecated 'yaml_content' field: {stale}. "
        "Update content to use pipeline_step rows and MCP CRUD tools instead."
    )


def test_org_fields_topic_exists(help_topics):
    """The 'org-fields' help topic must exist."""
    names = {t["name"] for t in help_topics}
    assert "org-fields" in names, (
        "'org-fields' help topic not found. "
        "Create it to document the project/tags/group org-field pattern."
    )


def test_org_fields_documents_project(help_topics):
    """The 'org-fields' topic must document the 'project' field."""
    topic = next((t for t in help_topics if t["name"] == "org-fields"), None)
    assert topic is not None, "org-fields topic missing"
    assert "project" in (topic.get("content") or ""), \
        "org-fields topic must document the 'project' org-field"


def test_org_fields_documents_tags(help_topics):
    """The 'org-fields' topic must document the 'tags' field."""
    topic = next((t for t in help_topics if t["name"] == "org-fields"), None)
    assert topic is not None, "org-fields topic missing"
    assert "tags" in (topic.get("content") or ""), \
        "org-fields topic must document the 'tags' org-field"


def test_org_fields_documents_group(help_topics):
    """The 'org-fields' topic must document the 'group' field."""
    topic = next((t for t in help_topics if t["name"] == "org-fields"), None)
    assert topic is not None, "org-fields topic missing"
    assert "group" in (topic.get("content") or ""), \
        "org-fields topic must document the 'group' (group_name) org-field"


def test_pipeline_persistence_topic_is_db_first(help_topics):
    """pipeline-persistence topic must describe DB tables, not YAML files."""
    topic = next((t for t in help_topics if t["name"] == "pipeline-persistence"), None)
    if topic is None:
        pytest.skip("pipeline-persistence topic not found")
    content = topic.get("content") or ""
    assert "pipeline_step" in content, \
        "pipeline-persistence topic must mention 'pipeline_step' table"
    assert "pipeline" in content, \
        "pipeline-persistence topic must describe the pipeline DB table"


def test_quick_start_does_not_mention_yaml_file_creation(help_topics):
    """quick-start topic must not instruct users to create YAML files."""
    topic = next((t for t in help_topics if t["name"] == "quick-start"), None)
    if topic is None:
        pytest.skip("quick-start topic not found")
    content = topic.get("content") or ""
    # Should not contain instructions to open/write YAML files manually
    assert "open(" not in content or "yaml" not in content.lower(), \
        "quick-start topic should not instruct users to manually create YAML files"


def test_triggers_topic_mentions_schedule_type(help_topics):
    """triggers topic must document the schedule trigger type with cron."""
    topic = next((t for t in help_topics if t["name"] == "triggers"), None)
    if topic is None:
        pytest.skip("triggers topic not found")
    content = topic.get("content") or ""
    assert "schedule" in content, "triggers topic must mention 'schedule' trigger type"
    assert "cron" in content.lower(), "triggers topic must mention cron syntax for schedule triggers"
