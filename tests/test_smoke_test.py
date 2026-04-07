from __future__ import annotations

from types import SimpleNamespace

import pytest

from brix.mcp_handlers.smoke import _handle_smoke_test
from brix.mcp_server import _HANDLERS


class _FakeDB:
    def list_pipelines(self) -> list[dict]:
        return [
            {"name": "good-pipeline", "project": "alpha"},
            {"name": "bad-pipeline", "project": "alpha"},
            {"name": "other-project", "project": "beta"},
        ]


class _FakePipelineStore:
    def load(self, name: str):
        if name == "good-pipeline":
            return SimpleNamespace(
                steps=[
                    SimpleNamespace(
                        id="step-ok",
                        config={"helper": "helper_ok", "connection": "main_conn"},
                    )
                ]
            )
        if name == "bad-pipeline":
            return SimpleNamespace(
                steps=[
                    SimpleNamespace(
                        id="step-bad",
                        config={
                            "helper": "helper_missing",
                            "connection": "missing_conn",
                            "pipeline": "missing-sub",
                        },
                    )
                ]
            )
        if name == "other-project":
            raise AssertionError("project filter/limit should exclude this pipeline")
        raise FileNotFoundError(name)


class _FakeValidationResult:
    def __init__(self, is_valid: bool, errors: list[str] | None = None):
        self.is_valid = is_valid
        self.errors = errors or []


class _FakeValidator:
    def validate(self, pipeline, level: str = "standard"):
        assert level == "quick"
        step_id = pipeline.steps[0].id
        if step_id == "step-ok":
            return _FakeValidationResult(True)
        return _FakeValidationResult(False, ["invalid config", "second", "third", "fourth"])


class _FakeHelperRegistry:
    def __init__(self, registry_path=None, db=None):
        self.db = db

    def get(self, name: str):
        if name == "helper_ok":
            return SimpleNamespace(code="print('ok')")
        return None


class _FakeConnectionManager:
    def __init__(self, db):
        self.db = db

    def list(self) -> list[dict]:
        return [{"name": "main_conn"}]


@pytest.mark.asyncio
async def test_smoke_test_handler_runs_checks(monkeypatch):
    monkeypatch.setattr("brix.db.BrixDB", _FakeDB)
    monkeypatch.setattr("brix.pipeline_store.PipelineStore", _FakePipelineStore)
    monkeypatch.setattr("brix.validator.PipelineValidator", _FakeValidator)
    monkeypatch.setattr("brix.helper_registry.HelperRegistry", _FakeHelperRegistry)
    monkeypatch.setattr("brix.connections.ConnectionManager", _FakeConnectionManager)

    result = await _handle_smoke_test({"project": "alpha", "limit": 2})

    assert result["success"] is False
    assert result["tested"] == 2
    assert result["passed"] == 1
    assert result["failed"] == 1

    good, bad = result["results"]
    assert good["pipeline"] == "good-pipeline"
    assert good["load"] == "pass"
    assert good["preflight"] == "pass"
    assert good["helpers"] == "pass"
    assert good["connections"] == "pass"
    assert good["sub_pipelines"] == "pass"
    assert good["issues"] == []

    assert bad["pipeline"] == "bad-pipeline"
    assert bad["load"] == "pass"
    assert bad["preflight"] == "fail"
    assert bad["helpers"] == "fail"
    assert bad["connections"] == "fail"
    assert bad["sub_pipelines"] == "fail"
    assert "invalid config" in bad["issues"]
    assert "Step step-bad: helper helper_missing not found" in bad["issues"]
    assert "Step step-bad: connection missing_conn not found" in bad["issues"]
    assert "Step step-bad: sub-pipeline missing-sub not found" in bad["issues"]
    assert "fourth" not in bad["issues"]


def test_smoke_test_handler_registered():
    assert _HANDLERS["brix__smoke_test"] is _handle_smoke_test
