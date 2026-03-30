"""Tests for pipeline_helpers join table population (T-BRIX-DBQUAL-01).

Covers:
- _extract_helper_refs extracts refs from top-level and nested steps
- _sync_pipeline_helpers populates join table correctly
- refresh_pipeline_deps works end-to-end
- Backfill migration v60
"""
import json
from pathlib import Path

import pytest
import yaml

from brix.db import BrixDB


@pytest.fixture
def db(tmp_path):
    """Return a BrixDB backed by a temporary file."""
    return BrixDB(db_path=tmp_path / "brix.db")


# ---------------------------------------------------------------------------
# _extract_helper_refs
# ---------------------------------------------------------------------------

class TestExtractHelperRefs:
    def test_top_level_helper(self):
        steps = [{"id": "s1", "type": "python", "helper": "my_helper"}]
        assert BrixDB._extract_helper_refs(steps) == {"my_helper"}

    def test_top_level_script(self):
        steps = [{"id": "s1", "type": "python", "script": "helpers/foo.py"}]
        assert BrixDB._extract_helper_refs(steps) == {"foo"}

    def test_params_helper(self):
        steps = [{"id": "s1", "type": "python", "params": {"helper": "bar_helper"}}]
        assert BrixDB._extract_helper_refs(steps) == {"bar_helper"}

    def test_params_script(self):
        steps = [{"id": "s1", "type": "python", "params": {"script": "/app/helpers/baz.py"}}]
        assert BrixDB._extract_helper_refs(steps) == {"baz"}

    def test_nested_repeat_sequence(self):
        steps = [
            {
                "id": "r1",
                "type": "repeat",
                "sequence": [
                    {"id": "s1", "type": "python", "helper": "inner_helper"},
                ],
            }
        ]
        assert BrixDB._extract_helper_refs(steps) == {"inner_helper"}

    def test_nested_choose_branches(self):
        steps = [
            {
                "id": "c1",
                "type": "choose",
                "choices": [
                    {
                        "when": "{{ true }}",
                        "steps": [
                            {"id": "s1", "type": "python", "helper": "branch_a"},
                        ],
                    },
                    {
                        "when": "{{ false }}",
                        "steps": [
                            {"id": "s2", "type": "python", "helper": "branch_b"},
                        ],
                    },
                ],
                "default_steps": [
                    {"id": "s3", "type": "python", "helper": "branch_default"},
                ],
            }
        ]
        refs = BrixDB._extract_helper_refs(steps)
        assert refs == {"branch_a", "branch_b", "branch_default"}

    def test_nested_parallel_sub_steps(self):
        steps = [
            {
                "id": "p1",
                "type": "parallel",
                "sub_steps": [
                    {"id": "s1", "type": "python", "helper": "par_a"},
                    {"id": "s2", "type": "python", "helper": "par_b"},
                ],
            }
        ]
        refs = BrixDB._extract_helper_refs(steps)
        assert refs == {"par_a", "par_b"}

    def test_multiple_refs_combined(self):
        steps = [
            {"id": "s1", "type": "python", "helper": "h1"},
            {"id": "s2", "type": "python", "script": "helpers/h2.py"},
            {"id": "s3", "type": "cli", "args": ["echo"]},  # no helper ref
        ]
        refs = BrixDB._extract_helper_refs(steps)
        assert refs == {"h1", "h2"}

    def test_empty_steps(self):
        assert BrixDB._extract_helper_refs([]) == set()

    def test_non_dict_steps_skipped(self):
        assert BrixDB._extract_helper_refs(["not_a_dict", 42]) == set()

    def test_deeply_nested(self):
        """Repeat inside parallel inside choose -- multi-level nesting."""
        steps = [
            {
                "id": "c1",
                "type": "choose",
                "choices": [
                    {
                        "when": "{{ true }}",
                        "steps": [
                            {
                                "id": "p1",
                                "type": "parallel",
                                "sub_steps": [
                                    {
                                        "id": "r1",
                                        "type": "repeat",
                                        "sequence": [
                                            {"id": "s1", "type": "python", "helper": "deep_helper"},
                                        ],
                                    }
                                ],
                            }
                        ],
                    }
                ],
            }
        ]
        assert BrixDB._extract_helper_refs(steps) == {"deep_helper"}

    def test_script_path_normalization(self):
        """Various script path formats should all normalize to bare name."""
        steps = [
            {"id": "s1", "type": "python", "script": "my_helper"},
            {"id": "s2", "type": "python", "script": "helpers/my_helper.py"},
            {"id": "s3", "type": "python", "script": "./helpers/my_helper.py"},
            {"id": "s4", "type": "python", "script": "/app/helpers/my_helper.py"},
        ]
        refs = BrixDB._extract_helper_refs(steps)
        assert refs == {"my_helper"}


# ---------------------------------------------------------------------------
# refresh_pipeline_deps (end-to-end with DB)
# ---------------------------------------------------------------------------

class TestRefreshPipelineDeps:
    def test_creates_join_rows(self, db):
        """refresh_pipeline_deps populates pipeline_helpers for known helpers."""
        db.upsert_helper("h1", "/h1.py")
        db.upsert_helper("h2", "/h2.py")

        pipeline_yaml = yaml.dump({
            "name": "test-pipe",
            "version": "1.0.0",
            "steps": [
                {"id": "s1", "type": "python", "helper": "h1"},
                {"id": "s2", "type": "python", "script": "helpers/h2.py"},
            ],
        })
        db.upsert_pipeline("test-pipe", "/test-pipe.yaml", yaml_content=pipeline_yaml)
        db.refresh_pipeline_deps("test-pipe")

        helpers = db.get_pipeline_helpers("test-pipe")
        names = {h["name"] for h in helpers}
        assert names == {"h1", "h2"}

    def test_removes_stale_rows(self, db):
        """Removing a helper ref from steps removes the join row."""
        db.upsert_helper("h1", "/h1.py")
        db.upsert_helper("h2", "/h2.py")

        # First: both helpers referenced
        yaml1 = yaml.dump({
            "name": "test-pipe", "version": "1.0.0",
            "steps": [
                {"id": "s1", "type": "python", "helper": "h1"},
                {"id": "s2", "type": "python", "helper": "h2"},
            ],
        })
        db.upsert_pipeline("test-pipe", "/test-pipe.yaml", yaml_content=yaml1)
        db.refresh_pipeline_deps("test-pipe")
        assert len(db.get_pipeline_helpers("test-pipe")) == 2

        # Second: only h1 referenced
        yaml2 = yaml.dump({
            "name": "test-pipe", "version": "1.0.1",
            "steps": [
                {"id": "s1", "type": "python", "helper": "h1"},
            ],
        })
        db.upsert_pipeline("test-pipe", "/test-pipe.yaml", yaml_content=yaml2)
        db.refresh_pipeline_deps("test-pipe")

        helpers = db.get_pipeline_helpers("test-pipe")
        assert len(helpers) == 1
        assert helpers[0]["name"] == "h1"

    def test_unknown_helpers_ignored(self, db):
        """Helper refs that don't exist in the helpers table are silently skipped."""
        yaml_content = yaml.dump({
            "name": "test-pipe", "version": "1.0.0",
            "steps": [
                {"id": "s1", "type": "python", "helper": "nonexistent"},
            ],
        })
        db.upsert_pipeline("test-pipe", "/test-pipe.yaml", yaml_content=yaml_content)
        db.refresh_pipeline_deps("test-pipe")
        assert db.get_pipeline_helpers("test-pipe") == []

    def test_unknown_pipeline_noop(self, db):
        """refresh_pipeline_deps on a non-existent pipeline does nothing."""
        db.refresh_pipeline_deps("ghost")  # Should not raise

    def test_nested_refs_in_repeat(self, db):
        """Helpers in nested repeat.sequence are discovered."""
        db.upsert_helper("nested_h", "/nested_h.py")
        yaml_content = yaml.dump({
            "name": "nest-pipe", "version": "1.0.0",
            "steps": [
                {
                    "id": "r1", "type": "repeat",
                    "sequence": [
                        {"id": "s1", "type": "python", "helper": "nested_h"},
                    ],
                },
            ],
        })
        db.upsert_pipeline("nest-pipe", "/nest.yaml", yaml_content=yaml_content)
        db.refresh_pipeline_deps("nest-pipe")
        helpers = db.get_pipeline_helpers("nest-pipe")
        assert len(helpers) == 1
        assert helpers[0]["name"] == "nested_h"


# ---------------------------------------------------------------------------
# Backfill migration
# ---------------------------------------------------------------------------

class TestBackfillMigration:
    def test_backfill_populates_existing_pipelines(self, db):
        """_backfill_pipeline_helpers processes all pipelines with yaml_content."""
        from brix.migrations import _backfill_pipeline_helpers

        db.upsert_helper("bf_helper", "/bf_helper.py")
        yaml_content = yaml.dump({
            "name": "bf-pipe", "version": "1.0.0",
            "steps": [
                {"id": "s1", "type": "python", "helper": "bf_helper"},
            ],
        })
        db.upsert_pipeline("bf-pipe", "/bf.yaml", yaml_content=yaml_content)

        # Verify no join rows yet
        assert db.get_pipeline_helpers("bf-pipe") == []

        # Run backfill
        _backfill_pipeline_helpers(db)

        helpers = db.get_pipeline_helpers("bf-pipe")
        assert len(helpers) == 1
        assert helpers[0]["name"] == "bf_helper"
