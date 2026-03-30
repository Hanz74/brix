"""Tests for T-BRIX-DBQUAL-03 — Project-level bundle import."""
import json
import tarfile
from pathlib import Path
from unittest.mock import MagicMock, call

import pytest

from brix.bundle import (
    MANIFEST_NAME,
    SECRET_MASK,
    ProjectExportManifest,
    ProjectImportResult,
    export_project,
    import_project,
)


# ---------------------------------------------------------------------------
# Fixtures — mock DB data (same as export tests)
# ---------------------------------------------------------------------------

_MOCK_PIPELINES = [
    {"id": "p1", "name": "test-pipeline-1", "project": "testproj", "tags": []},
    {"id": "p2", "name": "test-pipeline-2", "project": "testproj", "tags": []},
]

_MOCK_HELPERS = [
    {
        "id": "h1",
        "name": "helper_one",
        "project": "testproj",
        "description": "First helper",
        "requirements": ["httpx"],
        "input_schema": {"type": "object"},
        "output_schema": {"type": "object"},
        "tags": ["intake"],
        "group_name": "grp1",
    },
]

_MOCK_TRIGGERS = [
    {"id": "t1", "name": "trigger-a", "project": "testproj", "type": "cron",
     "config": {"schedule": "* * * * *"}, "enabled": True,
     "tags": [], "group_name": ""},
]

_MOCK_TRIGGER_GROUPS = [
    {"id": "tg1", "name": "group-a", "project": "testproj",
     "triggers": ["trigger-a"], "enabled": True, "description": "Test group",
     "tags": [], "group_name": ""},
]

_MOCK_VARIABLES = [
    {"name": "VAR_NORMAL", "value": "hello", "secret": False,
     "project": "testproj", "tags": [], "group_name": "", "description": "A var"},
    {"name": "VAR_SECRET", "value": "top-secret", "secret": True,
     "project": "testproj", "tags": [], "group_name": "", "description": "Secret"},
]

_MOCK_CONNECTOR_DEFS = [
    {"name": "outlook", "type": "mcp", "description": "M365 Outlook", "project": "testproj",
     "required_mcp_server": "m365", "required_mcp_tools": ["send-mail"],
     "output_schema": {}, "parameters": [], "related_pipelines": [], "related_helpers": [],
     "tags": [], "group_name": ""},
]

_MOCK_ALERT_RULES = [
    {"id": "ar1", "name": "high-failure-rate", "condition": "failure_rate > 0.5",
     "channel": "mattermost", "config": {}, "enabled": True,
     "project": "testproj", "tags": ["monitoring"], "group_name": ""},
]

_MOCK_PROFILES = [
    {"name": "staging", "config": {"timeout": 30}, "description": "Staging env",
     "project": "testproj", "tags": ["env"], "group_name": ""},
]

_MOCK_YAML_CONTENT = {
    "test-pipeline-1": (
        "name: test-pipeline-1\nversion: '1.0'\n"
        "credentials:\n  api_key:\n    env: API_KEY\n"
        "steps:\n  - id: step1\n    type: cli\n    args: ['echo', 'hi']\n"
    ),
    "test-pipeline-2": (
        "name: test-pipeline-2\nversion: '1.0'\n"
        "steps:\n  - id: step1\n    type: cli\n    args: ['echo', 'bye']\n"
    ),
}

_MOCK_HELPER_CODE = {
    "helper_one": "#!/usr/bin/env python3\nprint('hello')\n",
}


def _make_export_db():
    """Return a MagicMock that acts like BrixDB for project export."""
    db = MagicMock()
    db.list_pipelines.return_value = _MOCK_PIPELINES
    db.list_helpers.return_value = _MOCK_HELPERS
    db.trigger_list.return_value = _MOCK_TRIGGERS
    db.trigger_group_list.return_value = _MOCK_TRIGGER_GROUPS
    db.variable_list.return_value = _MOCK_VARIABLES
    db.connector_definitions_list.return_value = _MOCK_CONNECTOR_DEFS
    db.alert_rule_list.return_value = _MOCK_ALERT_RULES
    db.profile_list.return_value = _MOCK_PROFILES

    def _get_yaml(name):
        return _MOCK_YAML_CONTENT.get(name)
    db.get_pipeline_yaml_content.side_effect = _get_yaml

    def _get_code(name):
        return _MOCK_HELPER_CODE.get(name)
    db.get_helper_code.side_effect = _get_code

    return db


def _make_import_db(*, has_pipelines=False, has_helpers=False,
                    has_triggers=False, has_trigger_groups=False,
                    has_connectors=False, has_alert_rules=False,
                    has_profiles=False):
    """Return a MagicMock that acts like BrixDB for project import."""
    db = MagicMock()

    # get_pipeline_yaml_content: return None = doesn't exist
    if has_pipelines:
        db.get_pipeline_yaml_content.return_value = "existing yaml"
    else:
        db.get_pipeline_yaml_content.return_value = None

    # get_helper_code: return None = doesn't exist
    if has_helpers:
        db.get_helper_code.return_value = "existing code"
    else:
        db.get_helper_code.return_value = None

    # trigger_get: return None = doesn't exist
    if has_triggers:
        db.trigger_get.return_value = {"id": "existing", "name": "trigger-a"}
    else:
        db.trigger_get.return_value = None

    # trigger_group_get: return None = doesn't exist
    if has_trigger_groups:
        db.trigger_group_get.return_value = {"id": "existing", "name": "group-a"}
    else:
        db.trigger_group_get.return_value = None

    # connector_definitions_get: return None = doesn't exist
    if has_connectors:
        db.connector_definitions_get.return_value = {"name": "outlook"}
    else:
        db.connector_definitions_get.return_value = None

    # alert_rule_get: return None = doesn't exist
    if has_alert_rules:
        db.alert_rule_get.return_value = {"id": "ar1", "name": "high-failure-rate"}
    else:
        db.alert_rule_get.return_value = None

    # profile_get: return None = doesn't exist
    if has_profiles:
        db.profile_get.return_value = {"name": "staging", "config": {}}
    else:
        db.profile_get.return_value = None

    return db


def _create_export(tmp_path) -> Path:
    """Create an export archive for testing import."""
    output = tmp_path / "testproj.project.brix.tar.gz"
    export_db = _make_export_db()
    export_project("testproj", output, db=export_db)
    return output


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestImportCreatesAllEntities:
    """Test that import_project creates all entities from an export."""

    def test_imports_pipelines(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["pipelines"] == 2
        assert import_db.upsert_pipeline.call_count == 2
        # Check the pipeline names
        call_names = {c.kwargs.get("name") or c[1][0]
                      for c in import_db.upsert_pipeline.call_args_list}
        # upsert_pipeline is called with keyword args
        names_called = set()
        for c in import_db.upsert_pipeline.call_args_list:
            names_called.add(c.kwargs["name"])
        assert "test-pipeline-1" in names_called
        assert "test-pipeline-2" in names_called

    def test_imports_helpers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["helpers"] == 1
        assert import_db.upsert_helper.call_count == 1
        call_kw = import_db.upsert_helper.call_args_list[0].kwargs
        assert call_kw["name"] == "helper_one"
        assert call_kw["description"] == "First helper"
        assert call_kw["requirements"] == ["httpx"]
        assert "print('hello')" in call_kw["code"]

    def test_imports_triggers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["triggers"] == 1
        assert import_db.trigger_add.call_count == 1

    def test_imports_trigger_groups(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["trigger_groups"] == 1
        assert import_db.trigger_group_add.call_count == 1

    def test_imports_non_secret_variables(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        # Only VAR_NORMAL should be imported; VAR_SECRET has masked value
        assert result.imported["variables"] == 1
        assert import_db.variable_set.call_count == 1
        call_kw = import_db.variable_set.call_args_list[0].kwargs
        assert call_kw["name"] == "VAR_NORMAL"
        assert call_kw["value"] == "hello"

    def test_imports_connector_definitions(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["connector_definitions"] == 1
        assert import_db.connector_definitions_upsert.call_count == 1

    def test_imports_alert_rules(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["alert_rules"] == 1
        assert import_db.alert_rule_add.call_count == 1

    def test_imports_profiles(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.imported["profiles"] == 1
        assert import_db.profile_set.call_count == 1
        call_kw = import_db.profile_set.call_args_list[0].kwargs
        assert call_kw["name"] == "staging"
        assert call_kw["config"] == {"timeout": 30}


class TestDryRunDoesNotWrite:
    """Test that dry_run=True reports but does not modify DB."""

    def test_dry_run_counts(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db, dry_run=True)

        assert result.imported["pipelines"] == 2
        assert result.imported["helpers"] == 1
        assert result.imported["triggers"] == 1
        assert result.imported["trigger_groups"] == 1
        assert result.imported["variables"] == 1
        assert result.imported["connector_definitions"] == 1
        assert result.imported["alert_rules"] == 1
        assert result.imported["profiles"] == 1

    def test_dry_run_no_writes(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        import_project(archive, db=import_db, dry_run=True)

        import_db.upsert_pipeline.assert_not_called()
        import_db.upsert_helper.assert_not_called()
        import_db.trigger_add.assert_not_called()
        import_db.trigger_group_add.assert_not_called()
        import_db.variable_set.assert_not_called()
        import_db.connector_definitions_upsert.assert_not_called()
        import_db.alert_rule_add.assert_not_called()
        import_db.profile_set.assert_not_called()


class TestOnConflictSkip:
    """Test that on_conflict='skip' skips existing entities."""

    def test_skip_existing_pipelines(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_pipelines=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["pipelines"] == 0
        assert result.skipped["pipelines"] == 2
        import_db.upsert_pipeline.assert_not_called()

    def test_skip_existing_helpers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_helpers=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["helpers"] == 0
        assert result.skipped["helpers"] == 1
        import_db.upsert_helper.assert_not_called()

    def test_skip_existing_triggers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_triggers=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["triggers"] == 0
        assert result.skipped["triggers"] == 1
        import_db.trigger_add.assert_not_called()
        import_db.trigger_update.assert_not_called()

    def test_skip_existing_trigger_groups(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_trigger_groups=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["trigger_groups"] == 0
        assert result.skipped["trigger_groups"] == 1
        import_db.trigger_group_add.assert_not_called()
        import_db.trigger_group_update.assert_not_called()


class TestOnConflictOverwrite:
    """Test that on_conflict='overwrite' updates existing entities."""

    def test_overwrite_existing_pipelines(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_pipelines=True)

        result = import_project(archive, db=import_db, on_conflict="overwrite")

        # upsert_pipeline handles both insert and update
        assert result.imported["pipelines"] == 2
        assert import_db.upsert_pipeline.call_count == 2

    def test_overwrite_existing_helpers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_helpers=True)

        result = import_project(archive, db=import_db, on_conflict="overwrite")

        assert result.imported["helpers"] == 1
        assert import_db.upsert_helper.call_count == 1

    def test_overwrite_existing_triggers(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_triggers=True)

        result = import_project(archive, db=import_db, on_conflict="overwrite")

        assert result.imported["triggers"] == 1
        # When existing, should use trigger_update (not trigger_add)
        import_db.trigger_update.assert_called_once()
        import_db.trigger_add.assert_not_called()

    def test_overwrite_existing_trigger_groups(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_trigger_groups=True)

        result = import_project(archive, db=import_db, on_conflict="overwrite")

        assert result.imported["trigger_groups"] == 1
        import_db.trigger_group_update.assert_called_once()
        import_db.trigger_group_add.assert_not_called()

    def test_overwrite_existing_alert_rules(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_alert_rules=True)

        result = import_project(archive, db=import_db, on_conflict="overwrite")

        assert result.imported["alert_rules"] == 1
        import_db.alert_rule_update.assert_called_once()
        import_db.alert_rule_add.assert_not_called()


class TestSkipNewEntityTypes:
    """Test that on_conflict='skip' works for new entity types."""

    def test_skip_existing_connectors(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_connectors=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["connector_definitions"] == 0
        assert result.skipped["connector_definitions"] == 1
        import_db.connector_definitions_upsert.assert_not_called()

    def test_skip_existing_alert_rules(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_alert_rules=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["alert_rules"] == 0
        assert result.skipped["alert_rules"] == 1

    def test_skip_existing_profiles(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db(has_profiles=True)

        result = import_project(archive, db=import_db, on_conflict="skip")

        assert result.imported["profiles"] == 0
        assert result.skipped["profiles"] == 1
        import_db.profile_set.assert_not_called()


class TestSecretsSkipped:
    """Test that secret variables with masked values are not imported."""

    def test_masked_secrets_skipped(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        assert result.skipped["variables"] == 1
        # Only VAR_NORMAL imported, VAR_SECRET skipped
        assert result.imported["variables"] == 1
        # Verify variable_set was called only for VAR_NORMAL
        for c in import_db.variable_set.call_args_list:
            assert c.kwargs["name"] != "VAR_SECRET"


class TestMissingCredentialsReported:
    """Test that credential references from manifest are reported."""

    def test_credential_references_in_result(self, tmp_path):
        archive = _create_export(tmp_path)
        import_db = _make_import_db()

        result = import_project(archive, db=import_db)

        # test-pipeline-1 has credentials.api_key
        assert len(result.missing_credentials) > 0
        assert "test-pipeline-1:api_key" in result.missing_credentials


class TestProjectImportResult:
    """Test the ProjectImportResult data class."""

    def test_to_dict(self):
        r = ProjectImportResult(
            imported={"pipelines": 2, "helpers": 1},
            skipped={"pipelines": 0, "helpers": 0},
            errors=["something failed"],
            missing_credentials=["pipe:cred"],
        )
        d = r.to_dict()
        assert d["imported"]["pipelines"] == 2
        assert d["errors"] == ["something failed"]
        assert d["missing_credentials"] == ["pipe:cred"]


class TestInvalidArchive:
    """Test error handling for invalid archives."""

    def test_nonexistent_file(self, tmp_path):
        import_db = _make_import_db()
        result = import_project(
            tmp_path / "nonexistent.tar.gz", db=import_db
        )
        assert len(result.errors) > 0
        assert result.imported["pipelines"] == 0

    def test_empty_archive(self, tmp_path):
        archive = tmp_path / "empty.project.brix.tar.gz"
        # Create a valid but empty tar.gz with just a manifest
        with tarfile.open(archive, "w:gz") as tar:
            import io
            manifest = json.dumps({
                "project": "empty",
                "brix_version": "test",
                "created_at": "",
                "counts": {},
                "credential_references": {},
            }).encode()
            info = tarfile.TarInfo(name="manifest.json")
            info.size = len(manifest)
            tar.addfile(info, io.BytesIO(manifest))

        import_db = _make_import_db()
        result = import_project(archive, db=import_db)

        assert result.imported["pipelines"] == 0
        assert result.imported["helpers"] == 0
        assert len(result.errors) == 0
