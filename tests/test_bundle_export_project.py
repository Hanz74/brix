"""Tests for T-BRIX-DBQUAL-02 — Project-level bundle export."""
import json
import tarfile
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from brix.bundle import (
    MANIFEST_NAME,
    SECRET_MASK,
    ProjectExportManifest,
    export_project,
)


# ---------------------------------------------------------------------------
# Fixtures — mock DB data
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
        "input_schema": {},
        "output_schema": {},
        "tags": ["intake"],
        "group_name": "",
    },
]

_MOCK_TRIGGERS = [
    {"id": "t1", "name": "trigger-a", "project": "testproj", "type": "cron", "config": {}, "enabled": True},
    {"id": "t2", "name": "trigger-b", "project": "other", "type": "cron", "config": {}, "enabled": True},
]

_MOCK_TRIGGER_GROUPS = [
    {"id": "tg1", "name": "group-a", "project": "testproj", "triggers": ["trigger-a"], "enabled": True},
    {"id": "tg2", "name": "group-b", "project": "other", "triggers": ["trigger-b"], "enabled": True},
]

_MOCK_VARIABLES = [
    {"name": "VAR_NORMAL", "value": "hello", "secret": False, "project": "testproj", "tags": [], "group_name": ""},
    {"name": "VAR_SECRET", "value": "***SECRET***", "secret": True, "project": "testproj", "tags": [], "group_name": ""},
]

_MOCK_CONNECTOR_DEFS = [
    {"name": "outlook", "type": "mcp", "description": "M365 Outlook", "project": "testproj",
     "required_mcp_server": "m365", "required_mcp_tools": ["send-mail"],
     "output_schema": {}, "parameters": [], "related_pipelines": [], "related_helpers": [],
     "tags": [], "group_name": ""},
    {"name": "gmail", "type": "imap", "description": "Gmail", "project": "other",
     "tags": [], "group_name": ""},
]

_MOCK_ALERT_RULES = [
    {"id": "ar1", "name": "high-failure-rate", "condition": "failure_rate > 0.5",
     "channel": "mattermost", "config": {}, "enabled": True,
     "project": "testproj", "tags": ["monitoring"], "group_name": ""},
    {"id": "ar2", "name": "other-alert", "condition": "true",
     "channel": "email", "config": {}, "enabled": True,
     "project": "other", "tags": [], "group_name": ""},
]

_MOCK_PROFILES = [
    {"name": "staging", "config": {"timeout": 30}, "description": "Staging env",
     "project": "testproj", "tags": ["env"], "group_name": ""},
    {"name": "production", "config": {"timeout": 60}, "description": "Prod env",
     "project": "other", "tags": [], "group_name": ""},
]

_MOCK_YAML_CONTENT = {
    "test-pipeline-1": "name: test-pipeline-1\nversion: '1.0'\ncredentials:\n  api_key:\n    env: API_KEY\nsteps:\n  - id: step1\n    type: cli\n    args: ['echo', 'hi']\n",
    "test-pipeline-2": "name: test-pipeline-2\nversion: '1.0'\nsteps:\n  - id: step1\n    type: cli\n    args: ['echo', 'bye']\n",
}

_MOCK_HELPER_CODE = {
    "helper_one": "#!/usr/bin/env python3\nprint('hello')\n",
}


def _make_mock_db():
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


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


class TestExportProjectCreatesValidArchive:
    """Test that export_project creates a valid tar.gz with correct structure."""

    def test_creates_tar_gz(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        manifest = export_project("testproj", output, db=mock_db)

        assert output.exists()
        assert tarfile.is_tarfile(output)

    def test_archive_structure(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()

        assert MANIFEST_NAME in names
        assert "pipelines/test-pipeline-1.yaml" in names
        assert "pipelines/test-pipeline-2.yaml" in names
        assert "helpers/helper_one.py" in names
        assert "helpers/helper_one.metadata.json" in names
        assert "triggers/trigger-a.json" in names
        assert "trigger_groups/group-a.json" in names
        assert "variables/VAR_NORMAL.json" in names
        assert "variables/VAR_SECRET.json" in names
        assert "connector_definitions/outlook.json" in names
        assert "alert_rules/high-failure-rate.json" in names
        assert "profiles/staging.json" in names


class TestManifestCounts:
    """Test that manifest.json has correct entity counts."""

    def test_manifest_counts(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        manifest = export_project("testproj", output, db=mock_db)

        assert manifest.counts["pipelines"] == 2
        assert manifest.counts["helpers"] == 1
        assert manifest.counts["triggers"] == 1  # only testproj, not 'other'
        assert manifest.counts["trigger_groups"] == 1
        assert manifest.counts["variables"] == 2
        assert manifest.counts["connector_definitions"] == 1
        assert manifest.counts["alert_rules"] == 1
        assert manifest.counts["profiles"] == 1

    def test_manifest_in_archive_matches(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile(MANIFEST_NAME)
            data = json.loads(f.read())

        assert data["project"] == "testproj"
        assert data["counts"]["pipelines"] == 2
        assert data["counts"]["helpers"] == 1
        assert data["counts"]["triggers"] == 1
        assert data["counts"]["trigger_groups"] == 1
        assert data["counts"]["variables"] == 2
        assert data["counts"]["connector_definitions"] == 1
        assert data["counts"]["alert_rules"] == 1
        assert data["counts"]["profiles"] == 1


class TestAllEntityTypesIncluded:
    """Test that all entity types are present in the export."""

    def test_pipeline_yaml_content(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("pipelines/test-pipeline-1.yaml")
            content = f.read().decode("utf-8")

        assert "test-pipeline-1" in content
        assert "api_key" in content

    def test_helper_code_and_metadata(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            code_f = tar.extractfile("helpers/helper_one.py")
            code = code_f.read().decode("utf-8")
            meta_f = tar.extractfile("helpers/helper_one.metadata.json")
            meta = json.loads(meta_f.read())

        assert "print('hello')" in code
        assert meta["name"] == "helper_one"
        assert meta["requirements"] == ["httpx"]

    def test_trigger_included(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("triggers/trigger-a.json")
            data = json.loads(f.read())

        assert data["name"] == "trigger-a"
        assert data["project"] == "testproj"

    def test_trigger_from_other_project_excluded(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()

        assert "triggers/trigger-b.json" not in names
        assert "trigger_groups/group-b.json" not in names

    def test_trigger_group_included(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("trigger_groups/group-a.json")
            data = json.loads(f.read())

        assert data["name"] == "group-a"

    def test_connector_definition_included(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("connector_definitions/outlook.json")
            data = json.loads(f.read())

        assert data["name"] == "outlook"
        assert data["project"] == "testproj"

    def test_connector_from_other_project_excluded(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()

        assert "connector_definitions/gmail.json" not in names

    def test_alert_rule_included(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("alert_rules/high-failure-rate.json")
            data = json.loads(f.read())

        assert data["name"] == "high-failure-rate"
        assert data["project"] == "testproj"

    def test_alert_from_other_project_excluded(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()

        assert "alert_rules/other-alert.json" not in names

    def test_profile_included(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("profiles/staging.json")
            data = json.loads(f.read())

        assert data["name"] == "staging"
        assert data["project"] == "testproj"
        assert data["config"] == {"timeout": 30}

    def test_profile_from_other_project_excluded(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()

        assert "profiles/production.json" not in names

    def test_credential_references_in_manifest(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        manifest = export_project("testproj", output, db=mock_db)

        assert "test-pipeline-1" in manifest.credential_references
        assert "api_key" in manifest.credential_references["test-pipeline-1"]
        assert "test-pipeline-2" not in manifest.credential_references


class TestSecretsAreMasked:
    """Test that secret variable values never appear in the export."""

    def test_secret_value_masked_in_archive(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("variables/VAR_SECRET.json")
            data = json.loads(f.read())

        assert data["value"] == SECRET_MASK
        assert data["secret"] is True

    def test_non_secret_value_preserved(self, tmp_path):
        output = tmp_path / "test.project.brix.tar.gz"
        mock_db = _make_mock_db()

        export_project("testproj", output, db=mock_db)

        with tarfile.open(output, "r:gz") as tar:
            f = tar.extractfile("variables/VAR_NORMAL.json")
            data = json.loads(f.read())

        assert data["value"] == "hello"
        assert data["secret"] is False


class TestProjectExportManifest:
    """Test the ProjectExportManifest data class."""

    def test_roundtrip(self):
        m = ProjectExportManifest(
            project="test",
            brix_version="7.63.0",
            created_at="2026-03-30T12:00:00+00:00",
            counts={"pipelines": 2, "helpers": 1},
            credential_references={"pipe1": ["cred_a"]},
        )
        d = m.to_dict()
        m2 = ProjectExportManifest.from_dict(d)
        assert m2.project == "test"
        assert m2.counts == {"pipelines": 2, "helpers": 1}
        assert m2.credential_references == {"pipe1": ["cred_a"]}

    def test_empty_project_export(self, tmp_path):
        """Exporting a project with no entities should still create a valid archive."""
        output = tmp_path / "empty.project.brix.tar.gz"
        mock_db = MagicMock()
        mock_db.list_pipelines.return_value = []
        mock_db.list_helpers.return_value = []
        mock_db.trigger_list.return_value = []
        mock_db.trigger_group_list.return_value = []
        mock_db.variable_list.return_value = []
        mock_db.connector_definitions_list.return_value = []
        mock_db.alert_rule_list.return_value = []
        mock_db.profile_list.return_value = []

        manifest = export_project("empty", output, db=mock_db)

        assert output.exists()
        assert manifest.counts["pipelines"] == 0
        assert manifest.counts["helpers"] == 0
        assert manifest.counts["triggers"] == 0
        assert manifest.counts["trigger_groups"] == 0
        assert manifest.counts["variables"] == 0
        assert manifest.counts["connector_definitions"] == 0
        assert manifest.counts["alert_rules"] == 0
        assert manifest.counts["profiles"] == 0

        with tarfile.open(output, "r:gz") as tar:
            names = tar.getnames()
        assert MANIFEST_NAME in names
        assert len(names) == 1  # only manifest
