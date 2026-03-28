"""Tests for CredentialStore — Fernet-encrypted secrets with UUID references.

Covers:
- Schema creation
- add, get, list, update, delete, resolve
- UUID vs name lookup
- is_credential_uuid helper
- Invalid type rejection
- Duplicate name rejection
- Missing credential error
- Pipeline model: shorthand credential syntax coercion
- Context: UUID-based credential resolution
- MCP tools: add, list, get, update, delete
- CLI: add, list, delete commands
"""
import os
import sqlite3
import uuid
import warnings
from pathlib import Path
from unittest.mock import patch

import pytest


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture(autouse=True)
def set_master_key(monkeypatch):
    """Set a deterministic master key for all tests — avoids UserWarning about default key."""
    import secrets
    monkeypatch.setenv("BRIX_MASTER_KEY", "a" * 64)


@pytest.fixture
def store(tmp_path):
    """Return a CredentialStore backed by a temporary database."""
    from brix.credential_store import CredentialStore
    return CredentialStore(db_path=tmp_path / "credentials.db")


# ---------------------------------------------------------------------------
# Schema
# ---------------------------------------------------------------------------

class TestSchema:
    def test_db_created_on_init(self, tmp_path):
        from brix.credential_store import CredentialStore
        db_path = tmp_path / "creds.db"
        assert not db_path.exists()
        CredentialStore(db_path=db_path)
        assert db_path.exists()

    def test_credentials_table_exists(self, store):
        with store._connect() as conn:
            tables = {
                row[0]
                for row in conn.execute(
                    "SELECT name FROM sqlite_master WHERE type='table'"
                ).fetchall()
            }
        assert "credentials" in tables

    def test_db_path_creates_parent_dirs(self, tmp_path):
        from brix.credential_store import CredentialStore
        nested = tmp_path / "a" / "b" / "creds.db"
        CredentialStore(db_path=nested)
        assert nested.exists()


# ---------------------------------------------------------------------------
# add / get
# ---------------------------------------------------------------------------

class TestAddGet:
    def test_add_returns_uuid(self, store):
        cred_id = store.add("my-key", "api-key", "secret123")
        assert cred_id
        # Must be a valid UUID
        uuid.UUID(cred_id)

    def test_get_returns_metadata_no_value(self, store):
        cred_id = store.add("my-key", "api-key", "secret123")
        meta = store.get(cred_id)
        assert meta["id"] == cred_id
        assert meta["name"] == "my-key"
        assert meta["type"] == "api-key"
        assert "created_at" in meta
        assert "updated_at" in meta
        assert "value" not in meta
        assert "encrypted_value" not in meta

    def test_get_by_name(self, store):
        store.add("named-key", "oauth2", "token")
        meta = store.get("named-key")
        assert meta["name"] == "named-key"
        assert meta["type"] == "oauth2"

    def test_get_not_found_raises(self, store):
        from brix.credential_store import CredentialNotFoundError
        with pytest.raises(CredentialNotFoundError):
            store.get("nonexistent")

    def test_add_all_types(self, store):
        for cred_type in ("api-key", "oauth2", "basic-auth"):
            cred_id = store.add(f"cred-{cred_type}", cred_type, "value")
            meta = store.get(cred_id)
            assert meta["type"] == cred_type

    def test_add_invalid_type_raises(self, store):
        with pytest.raises(ValueError, match="Invalid credential type"):
            store.add("bad-cred", "bearer-token", "value")

    def test_add_duplicate_name_raises(self, store):
        store.add("dupe", "api-key", "v1")
        with pytest.raises(sqlite3.IntegrityError):
            store.add("dupe", "api-key", "v2")

    def test_value_is_encrypted_in_db(self, store, tmp_path):
        """Raw DB value is NOT plaintext."""
        db_path = tmp_path / "creds.db"
        s = type(store)(db_path=db_path)
        s.add("enc-test", "api-key", "my-plaintext-secret")
        # Read directly from SQLite
        conn = sqlite3.connect(str(db_path))
        row = conn.execute("SELECT encrypted_value FROM credentials WHERE name='enc-test'").fetchone()
        conn.close()
        assert row is not None
        assert "my-plaintext-secret" not in row[0]  # Not stored as plaintext


# ---------------------------------------------------------------------------
# list
# ---------------------------------------------------------------------------

class TestList:
    def test_list_empty(self, store):
        assert store.list() == []

    def test_list_returns_metadata_no_values(self, store):
        store.add("a", "api-key", "val-a")
        store.add("b", "oauth2", "val-b")
        items = store.list()
        assert len(items) == 2
        for item in items:
            assert "value" not in item
            assert "encrypted_value" not in item

    def test_list_sorted_by_name(self, store):
        store.add("zebra", "api-key", "v")
        store.add("alpha", "api-key", "v")
        store.add("middle", "api-key", "v")
        items = store.list()
        names = [i["name"] for i in items]
        assert names == sorted(names)


# ---------------------------------------------------------------------------
# update
# ---------------------------------------------------------------------------

class TestUpdate:
    def test_update_value(self, store):
        cred_id = store.add("rotatable", "api-key", "old-secret")
        store.update(cred_id, value="new-secret")
        # Resolve still works
        resolved = store.resolve(cred_id)
        assert resolved == "new-secret"

    def test_update_name(self, store):
        cred_id = store.add("old-name", "api-key", "val")
        store.update(cred_id, name="new-name")
        meta = store.get(cred_id)
        assert meta["name"] == "new-name"

    def test_update_by_name(self, store):
        store.add("update-by-name", "api-key", "original")
        store.update("update-by-name", value="updated")
        resolved = store.resolve("update-by-name")
        assert resolved == "updated"

    def test_update_not_found_raises(self, store):
        from brix.credential_store import CredentialNotFoundError
        with pytest.raises(CredentialNotFoundError):
            store.update("ghost", value="x")

    def test_update_returns_metadata_no_value(self, store):
        cred_id = store.add("u", "api-key", "v")
        meta = store.update(cred_id, value="new")
        assert "value" not in meta
        assert "encrypted_value" not in meta
        assert meta["id"] == cred_id

    def test_update_timestamps(self, store):
        cred_id = store.add("ts-test", "api-key", "v")
        original = store.get(cred_id)["updated_at"]
        import time
        time.sleep(0.01)
        store.update(cred_id, value="new-v")
        updated = store.get(cred_id)["updated_at"]
        assert updated >= original


# ---------------------------------------------------------------------------
# delete
# ---------------------------------------------------------------------------

class TestDelete:
    def test_delete_by_uuid(self, store):
        cred_id = store.add("to-del", "api-key", "v")
        assert store.delete(cred_id) is True
        from brix.credential_store import CredentialNotFoundError
        with pytest.raises(CredentialNotFoundError):
            store.get(cred_id)

    def test_delete_by_name(self, store):
        store.add("name-del", "api-key", "v")
        assert store.delete("name-del") is True
        assert store.list() == []

    def test_delete_not_found_returns_false(self, store):
        assert store.delete("nonexistent") is False

    def test_delete_is_idempotent(self, store):
        store.add("del-once", "api-key", "v")
        store.delete("del-once")
        assert store.delete("del-once") is False


# ---------------------------------------------------------------------------
# resolve (internal)
# ---------------------------------------------------------------------------

class TestResolve:
    def test_resolve_by_uuid(self, store):
        cred_id = store.add("api", "api-key", "my-secret-value")
        resolved = store.resolve(cred_id)
        assert resolved == "my-secret-value"

    def test_resolve_by_name(self, store):
        store.add("named", "api-key", "resolved-val")
        resolved = store.resolve("named")
        assert resolved == "resolved-val"

    def test_resolve_not_found_raises(self, store):
        from brix.credential_store import CredentialNotFoundError
        with pytest.raises(CredentialNotFoundError):
            store.resolve("ghost-uuid")

    def test_resolve_roundtrip(self, store):
        """Encrypt then decrypt gives back original value."""
        original = "super-secret-key-12345!@#"
        cred_id = store.add("roundtrip", "api-key", original)
        assert store.resolve(cred_id) == original


# ---------------------------------------------------------------------------
# is_credential_uuid helper
# ---------------------------------------------------------------------------

class TestIsCredentialUuid:
    def test_valid_raw_uuid(self):
        from brix.credential_store import is_credential_uuid
        valid_uuid = str(uuid.uuid4())
        assert is_credential_uuid(valid_uuid) is True

    def test_valid_prefixed_uuid(self):
        from brix.credential_store import is_credential_uuid
        prefixed = f"cred-{uuid.uuid4()}"
        assert is_credential_uuid(prefixed) is True

    def test_plain_env_var_name(self):
        from brix.credential_store import is_credential_uuid
        assert is_credential_uuid("MY_API_KEY") is False

    def test_partial_uuid_not_valid(self):
        from brix.credential_store import is_credential_uuid
        assert is_credential_uuid("cred-not-a-uuid") is False

    def test_empty_string(self):
        from brix.credential_store import is_credential_uuid
        assert is_credential_uuid("") is False


# ---------------------------------------------------------------------------
# Encryption key management
# ---------------------------------------------------------------------------

class TestEncryption:
    def test_default_key_emits_warning(self, tmp_path, monkeypatch):
        monkeypatch.delenv("BRIX_MASTER_KEY", raising=False)
        from brix.credential_store import CredentialStore
        store = CredentialStore(db_path=tmp_path / "creds.db")
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            store.add("warn-test", "api-key", "val")
            assert any("BRIX_MASTER_KEY" in str(warning.message) for warning in w)

    def test_hex_master_key_used(self, tmp_path, monkeypatch):
        import secrets
        monkeypatch.setenv("BRIX_MASTER_KEY", secrets.token_hex(32))
        from brix.credential_store import CredentialStore
        store = CredentialStore(db_path=tmp_path / "creds.db")
        cred_id = store.add("k", "api-key", "val")
        assert store.resolve(cred_id) == "val"

    def test_wrong_key_fails_decrypt(self, tmp_path, monkeypatch):
        """Data encrypted with key A cannot be decrypted with key B."""
        from cryptography.fernet import InvalidToken

        monkeypatch.setenv("BRIX_MASTER_KEY", "a" * 64)
        from brix.credential_store import CredentialStore
        store = CredentialStore(db_path=tmp_path / "creds.db")
        cred_id = store.add("k", "api-key", "secret")

        monkeypatch.setenv("BRIX_MASTER_KEY", "b" * 64)
        with pytest.raises(InvalidToken):
            store.resolve(cred_id)


# ---------------------------------------------------------------------------
# Pipeline model: shorthand credential coercion
# ---------------------------------------------------------------------------

class TestPipelineModelCoercion:
    def test_string_credential_coerced_to_credentialref(self):
        """credentials: {MY_KEY: "cred-uuid"} → {MY_KEY: {env: "cred-uuid"}}"""
        from brix.models import Pipeline
        raw = {
            "name": "test",
            "version": "1.0.0",
            "credentials": {"MY_KEY": str(uuid.uuid4())},
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        }
        pipeline = Pipeline.model_validate(raw)
        assert "MY_KEY" in pipeline.credentials
        assert pipeline.credentials["MY_KEY"].env == raw["credentials"]["MY_KEY"]

    def test_dict_credential_format_still_works(self):
        """credentials: {MY_KEY: {env: "MY_ENV_VAR"}} still works."""
        from brix.models import Pipeline
        raw = {
            "name": "test",
            "version": "1.0.0",
            "credentials": {"MY_KEY": {"env": "MY_ENV_VAR"}},
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        }
        pipeline = Pipeline.model_validate(raw)
        assert pipeline.credentials["MY_KEY"].env == "MY_ENV_VAR"


# ---------------------------------------------------------------------------
# Context: UUID-based credential resolution
# ---------------------------------------------------------------------------

class TestContextResolution:
    def test_uuid_credential_resolved_from_store(self, tmp_path, monkeypatch):
        """When cred.env is a UUID, PipelineContext resolves from CredentialStore."""
        from brix.credential_store import CredentialStore
        from brix.models import Pipeline
        import brix.context as _ctx_mod

        store = CredentialStore(db_path=tmp_path / "creds.db")
        cred_id = store.add("my-secret", "api-key", "plaintext-value")

        # Patch CredentialStore in context module to use our test DB
        monkeypatch.setattr(_ctx_mod, "CredentialStore", lambda: store)

        pipeline = Pipeline.model_validate({
            "name": "test",
            "version": "1.0.0",
            "credentials": {"MY_API_KEY": cred_id},
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        })
        from brix.context import PipelineContext
        ctx = PipelineContext.from_pipeline(pipeline)
        assert ctx.credentials["MY_API_KEY"] == "plaintext-value"

    def test_env_var_credential_still_works(self, monkeypatch):
        """When cred.env is a plain env var name, resolution from os.environ still works."""
        from brix.models import Pipeline
        from brix.context import PipelineContext

        monkeypatch.setenv("MY_REAL_ENV_VAR", "env-value-123")

        pipeline = Pipeline.model_validate({
            "name": "test",
            "version": "1.0.0",
            "credentials": {"MY_KEY": {"env": "MY_REAL_ENV_VAR"}},
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        })
        ctx = PipelineContext.from_pipeline(pipeline)
        assert ctx.credentials["MY_KEY"] == "env-value-123"

    def test_missing_uuid_credential_warns_and_uses_empty(self, tmp_path, monkeypatch):
        """Missing UUID credential logs a warning and falls back to empty string."""
        from brix.credential_store import CredentialStore
        from brix.models import Pipeline
        from brix.context import PipelineContext
        import brix.context as _ctx_mod

        store = CredentialStore(db_path=tmp_path / "creds.db")
        missing_uuid = str(uuid.uuid4())

        monkeypatch.setattr(_ctx_mod, "CredentialStore", lambda: store)

        pipeline = Pipeline.model_validate({
            "name": "test",
            "version": "1.0.0",
            "credentials": {"MY_KEY": missing_uuid},
            "steps": [{"id": "s1", "type": "cli", "args": ["echo"]}],
        })
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            ctx = PipelineContext.from_pipeline(pipeline)
            assert ctx.credentials["MY_KEY"] == ""
            assert any("not found" in str(warning.message) for warning in w)

    def test_credentials_not_in_run_json(self, tmp_path):
        """save_run_metadata NEVER writes credentials to disk (security invariant)."""
        from brix.context import PipelineContext
        import json

        ctx = PipelineContext(
            pipeline_input={"x": 1},
            credentials={"MY_KEY": "super-secret"},
            workdir=tmp_path / "run",
        )
        ctx.save_run_metadata("test-pipeline", "running")

        run_json = (tmp_path / "run" / "run.json").read_text()
        assert "super-secret" not in run_json
        data = json.loads(run_json)
        assert "credentials" not in data


# ---------------------------------------------------------------------------
# MCP tools
# ---------------------------------------------------------------------------

class TestMcpTools:
    @pytest.fixture
    def patched_store(self, tmp_path, monkeypatch):
        from brix.credential_store import CredentialStore
        store = CredentialStore(db_path=tmp_path / "creds.db")
        # Patch where CredentialStore is actually called (in the handler module)
        monkeypatch.setattr("brix.mcp_handlers.credentials.CredentialStore", lambda: store)
        return store

    async def test_credential_add_returns_uuid(self, patched_store):
        from brix.mcp_server import _handle_credential_add

        result = await _handle_credential_add({
            "name": "my-api-key",
            "type": "api-key",
            "value": "secret-value",
        })
        assert result["success"] is True
        assert "id" in result
        uuid.UUID(result["id"])
        assert "note" in result
        assert "value" not in str(result.get("id", ""))  # UUID, not the value

    async def test_credential_add_value_not_in_response(self, patched_store):
        from brix.mcp_server import _handle_credential_add

        result = await _handle_credential_add({
            "name": "hidden-key",
            "type": "api-key",
            "value": "super-secret-123",
        })
        # The plaintext value must NOT appear anywhere in the response
        response_str = str(result)
        assert "super-secret-123" not in response_str

    async def test_credential_list_no_values(self, patched_store):
        from brix.mcp_server import _handle_credential_add, _handle_credential_list

        await _handle_credential_add({"name": "k1", "type": "api-key", "value": "v1"})
        await _handle_credential_add({"name": "k2", "type": "oauth2", "value": "v2"})

        result = await _handle_credential_list({})
        assert result["success"] is True
        assert result["count"] == 2
        for item in result["credentials"]:
            assert "value" not in item
            assert "encrypted_value" not in item

    async def test_credential_get_metadata_only(self, patched_store):
        from brix.mcp_server import _handle_credential_add, _handle_credential_get

        add_result = await _handle_credential_add({
            "name": "meta-only",
            "type": "basic-auth",
            "value": "password",
        })
        cred_id = add_result["id"]

        get_result = await _handle_credential_get({"id_or_name": cred_id})
        assert get_result["success"] is True
        assert get_result["name"] == "meta-only"
        assert get_result["type"] == "basic-auth"
        assert "value" not in get_result
        assert "password" not in str(get_result)

    async def test_credential_get_not_found(self, patched_store):
        from brix.mcp_server import _handle_credential_get

        result = await _handle_credential_get({"id_or_name": "ghost"})
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    async def test_credential_update(self, patched_store):
        from brix.mcp_server import _handle_credential_add, _handle_credential_update

        add_result = await _handle_credential_add({
            "name": "to-update",
            "type": "api-key",
            "value": "old-value",
        })
        cred_id = add_result["id"]

        update_result = await _handle_credential_update({
            "id_or_name": cred_id,
            "value": "new-value",
        })
        assert update_result["success"] is True
        assert "new-value" not in str(update_result)  # value never returned

    async def test_credential_delete(self, patched_store):
        from brix.mcp_server import _handle_credential_add, _handle_credential_delete, _handle_credential_get

        add_result = await _handle_credential_add({
            "name": "to-delete",
            "type": "api-key",
            "value": "val",
        })
        cred_id = add_result["id"]

        del_result = await _handle_credential_delete({"id_or_name": cred_id})
        assert del_result["success"] is True

        get_result = await _handle_credential_get({"id_or_name": cred_id})
        assert get_result["success"] is False

    async def test_credential_add_missing_params(self):
        from brix.mcp_server import _handle_credential_add

        result = await _handle_credential_add({"name": "", "type": "api-key", "value": "v"})
        assert result["success"] is False

        result = await _handle_credential_add({"name": "n", "type": "bad-type", "value": "v"})
        assert result["success"] is False

    async def test_credential_add_invalid_type(self, patched_store):
        from brix.mcp_server import _handle_credential_add

        result = await _handle_credential_add({
            "name": "bad",
            "type": "invalid-type",
            "value": "v",
        })
        assert result["success"] is False
        assert "invalid" in result["error"].lower()


# ---------------------------------------------------------------------------
# CLI commands
# ---------------------------------------------------------------------------

class TestCli:
    def test_credential_add_cli(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main
        from brix.credential_store import CredentialStore

        db_path = tmp_path / "creds.db"
        monkeypatch.setattr("brix.credential_store.DEFAULT_DB_PATH", db_path)

        runner = CliRunner()
        result = runner.invoke(main, [
            "credential", "add", "my-key",
            "--type", "api-key",
            "--value", "my-secret",
        ])
        assert result.exit_code == 0
        # UUID should be printed to stdout
        output = result.output
        lines = [l for l in output.splitlines() if l.strip()]
        # At least one UUID-like line
        uuid_found = any(
            len(part) == 36 and part.count("-") == 4
            for line in lines
            for part in line.split()
        )
        assert uuid_found, f"No UUID in output: {output}"

    def test_credential_list_cli(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main
        from brix.credential_store import CredentialStore

        db_path = tmp_path / "creds.db"
        monkeypatch.setattr("brix.credential_store.DEFAULT_DB_PATH", db_path)

        # Pre-populate
        store = CredentialStore(db_path=db_path)
        store.add("list-test-cred", "api-key", "val")

        runner = CliRunner()
        result = runner.invoke(main, ["credential", "list"])
        assert result.exit_code == 0
        assert "list-test-cred" in result.output

    def test_credential_delete_cli(self, tmp_path, monkeypatch):
        from click.testing import CliRunner
        from brix.cli import main
        from brix.credential_store import CredentialStore

        db_path = tmp_path / "creds.db"
        monkeypatch.setattr("brix.credential_store.DEFAULT_DB_PATH", db_path)

        store = CredentialStore(db_path=db_path)
        store.add("delete-me", "api-key", "val")

        runner = CliRunner()
        result = runner.invoke(main, ["credential", "delete", "delete-me", "--yes"])
        assert result.exit_code == 0
        assert store.list() == []
