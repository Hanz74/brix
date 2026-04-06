"""Tests for T-BRIX-FDB-04: DB-backed environment profiles."""

import os

from brix.context import PipelineContext
from brix.db import BrixDB
from brix.loader import PipelineLoader
from brix.profiles import BRIX_PROFILE_ENV, ProfileManager, ProfileNotFoundError


def _patch_db(monkeypatch, tmp_path):
    db_path = tmp_path / "brix.db"
    monkeypatch.setattr("brix.db.BRIX_DB_PATH", db_path)
    return BrixDB(db_path=db_path)


def test_profile_manager_get_set_via_db(tmp_path, monkeypatch):
    db = _patch_db(monkeypatch, tmp_path)
    mgr = ProfileManager()

    mgr.save_profile("dev", env={"API_KEY": "dev-secret"}, input_defaults={"limit": 10})

    row = db.get_env_profile("dev")
    assert row is not None
    assert row["env"] == {"API_KEY": "dev-secret"}
    assert row["input_defaults"] == {"limit": 10}

    profile = mgr.get_profile("dev")
    assert profile["env"]["API_KEY"] == "dev-secret"
    assert profile["input_defaults"]["limit"] == 10


def test_default_profile_resolution_from_db(tmp_path, monkeypatch):
    db = _patch_db(monkeypatch, tmp_path)
    mgr = ProfileManager()

    db.upsert_env_profile("dev", env={"ENV_NAME": "dev"}, is_default=True)
    db.upsert_env_profile("prod", env={"ENV_NAME": "prod"}, is_default=False)

    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)
    assert mgr.get_default() == "dev"
    assert mgr.active_profile_name() == "dev"

    mgr.set_default("prod")
    assert mgr.get_default() == "prod"
    assert db.get_default_env_profile()["name"] == "prod"
    assert db.get_env_profile("dev")["is_default"] is False


def test_context_applies_db_profile_env_and_input_defaults(tmp_path, monkeypatch):
    db = _patch_db(monkeypatch, tmp_path)
    monkeypatch.setattr("brix.context.WORKDIR_BASE", tmp_path / "runs")
    db.upsert_env_profile(
        "staging",
        env={"PROFILE_TOKEN": "abc123"},
        input_defaults={"limit": 25},
        is_default=True,
    )

    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)
    monkeypatch.delenv("PROFILE_TOKEN", raising=False)

    pipeline = PipelineLoader().load_from_string(
        """
name: db-profile-test
input:
  limit:
    type: integer
credentials:
  token:
    env: PROFILE_TOKEN
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    )

    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx._active_profile == "staging"
    assert ctx.input["limit"] == 25
    assert ctx.credentials["token"] == "abc123"
    assert os.environ["PROFILE_TOKEN"] == "abc123"


def test_profile_manager_missing_profile_raises(tmp_path, monkeypatch):
    _patch_db(monkeypatch, tmp_path)
    mgr = ProfileManager()

    try:
        mgr.get_profile("missing")
        assert False, "expected ProfileNotFoundError"
    except ProfileNotFoundError:
        pass
