"""Tests for T-BRIX-LINT-01: credential lint checks CredentialStore for UUIDs.

Covers:
- UUID credential found in store → no warning, check added
- ENV credential set in environment → no warning, check added
- UUID not in store → warning
- ENV not set → warning
"""
import os
import uuid
from unittest.mock import patch, MagicMock

import pytest

from brix.models import Pipeline, CredentialRef, Step
from brix.validator import PipelineValidator
from brix.credential_store import CredentialNotFoundError


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_master_key(monkeypatch):
    """Deterministic master key — avoids UserWarning about default key."""
    monkeypatch.setenv("BRIX_MASTER_KEY", "b" * 64)


def _dummy_step() -> Step:
    """Minimal valid step (Pipeline requires at least one)."""
    return Step(id="noop", type="flow.set", config={"value": 1})


def _make_pipeline_with_credential(env_ref: str) -> Pipeline:
    """Return a minimal Pipeline with one credential pointing to env_ref."""
    return Pipeline(
        name="test-cred-pipeline",
        steps=[_dummy_step()],
        credentials={"MY_KEY": CredentialRef(env=env_ref)},
    )


# ---------------------------------------------------------------------------
# UUID credentials (CredentialStore path)
# ---------------------------------------------------------------------------

class TestUUIDCredential:
    def test_uuid_found_in_store_no_warning(self, tmp_path):
        """UUID credential present in CredentialStore → check added, no warning."""
        from brix.credential_store import CredentialStore

        store = CredentialStore(db_path=tmp_path / "credentials.db")
        cred_id = store.add("my-api-key", "api-key", "secret-value")

        pipeline = _make_pipeline_with_credential(cred_id)

        with patch("brix.credential_store.CredentialStore", return_value=store):
            result = PipelineValidator().validate(pipeline)

        assert not result.warnings, f"Expected no warnings, got: {result.warnings}"
        assert any("found in store" in c for c in result.checks), (
            f"Expected 'found in store' check, got: {result.checks}"
        )

    def test_uuid_not_in_store_warning(self, tmp_path):
        """UUID credential NOT in CredentialStore → warning emitted."""
        from brix.credential_store import CredentialStore

        # Empty store — nothing added
        store = CredentialStore(db_path=tmp_path / "credentials.db")

        missing_uuid = str(uuid.uuid4())
        pipeline = _make_pipeline_with_credential(missing_uuid)

        with patch("brix.credential_store.CredentialStore", return_value=store):
            result = PipelineValidator().validate(pipeline)

        cred_warnings = [w for w in result.warnings if "NOT FOUND in credential store" in w]
        assert cred_warnings, (
            f"Expected 'NOT FOUND in credential store' warning, got warnings: {result.warnings}"
        )

    def test_cred_uuid_no_false_positive(self, tmp_path):
        """A valid UUID that IS in the store produces no warnings."""
        from brix.credential_store import CredentialStore

        store = CredentialStore(db_path=tmp_path / "credentials.db")
        cred_id = store.add("prefixed-key", "api-key", "top-secret")

        pipeline = _make_pipeline_with_credential(cred_id)

        with patch("brix.credential_store.CredentialStore", return_value=store):
            result = PipelineValidator().validate(pipeline)

        assert not result.warnings, f"Expected no warnings, got: {result.warnings}"


# ---------------------------------------------------------------------------
# ENV credentials (os.environ path)
# ---------------------------------------------------------------------------

class TestEnvCredential:
    def test_env_set_no_warning(self, monkeypatch):
        """ENV var credential that IS set → check added, no warning."""
        monkeypatch.setenv("MY_SECRET_TOKEN", "hunter2")

        pipeline = _make_pipeline_with_credential("MY_SECRET_TOKEN")
        result = PipelineValidator().validate(pipeline)

        cred_warnings = [w for w in result.warnings if "MY_SECRET_TOKEN" in w]
        assert not cred_warnings, f"Expected no credential warnings, got: {cred_warnings}"
        assert any("MY_SECRET_TOKEN" in c for c in result.checks), (
            f"Expected check for MY_SECRET_TOKEN, got: {result.checks}"
        )

    def test_env_not_set_warning(self, monkeypatch):
        """ENV var credential that is NOT set → warning emitted."""
        env_var = "BRIX_TEST_MISSING_VAR_XYZ"
        monkeypatch.delenv(env_var, raising=False)

        pipeline = _make_pipeline_with_credential(env_var)
        result = PipelineValidator().validate(pipeline)

        cred_warnings = [w for w in result.warnings if env_var in w and "NOT SET" in w]
        assert cred_warnings, (
            f"Expected 'NOT SET' warning for {env_var}, got warnings: {result.warnings}"
        )


# ---------------------------------------------------------------------------
# Mixed — multiple credentials
# ---------------------------------------------------------------------------

class TestMixedCredentials:
    def test_one_uuid_one_env(self, tmp_path, monkeypatch):
        """One UUID cred (found) + one ENV cred (set) → no warnings."""
        from brix.credential_store import CredentialStore

        store = CredentialStore(db_path=tmp_path / "credentials.db")
        cred_id = store.add("mixed-key", "api-key", "value")

        monkeypatch.setenv("MY_ENV_VAR", "present")

        pipeline = Pipeline(
            name="mixed-creds",
            steps=[_dummy_step()],
            credentials={
                "UUID_KEY": CredentialRef(env=cred_id),
                "ENV_KEY": CredentialRef(env="MY_ENV_VAR"),
            },
        )

        with patch("brix.credential_store.CredentialStore", return_value=store):
            result = PipelineValidator().validate(pipeline)

        assert not result.warnings, f"Expected no warnings, got: {result.warnings}"
