"""Tests for Secret Variables — T-BRIX-DB-26.

Covers:
- variable_set with secret=True → value is Fernet-encrypted in DB
- variable_get secret → decrypts transparently
- variable_list secret → value shown as '***SECRET***', secret=True flag
- variable_get_raw → returns raw encrypted value + secret flag
- MCP handler: set_variable with secret=true
- MCP handler: get_variable secret → no value returned
- MCP handler: list_variables → '***SECRET***' for secrets
- Jinja2 context: {{ var.secret_name }} → decrypted plaintext
- Jinja2 context: _secret_values set populated with plaintext
- Redaction: _redact_secret_values replaces all occurrences
- Redaction: non-secret variables not redacted
- Non-secret variables work unchanged after secret feature added
"""
import asyncio
import json
import warnings
from pathlib import Path

import pytest

from brix.db import BrixDB
from brix.context import PipelineContext


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def db(tmp_path):
    return BrixDB(db_path=tmp_path / "brix_secret.db")


@pytest.fixture
def patched_db(tmp_path, monkeypatch):
    """DB fixture that also patches BrixDB.__init__ so context.py uses it."""
    test_db_path = tmp_path / "brix_secret_ctx.db"
    original_init = BrixDB.__init__

    def patched_init(self, db_path=None):
        original_init(self, db_path=test_db_path)

    monkeypatch.setattr(BrixDB, "__init__", patched_init)
    db = BrixDB()
    return db


# ===========================================================================
# Part 1: DB Layer — variable_set with secret=True
# ===========================================================================

class TestSecretVariableDB:
    def test_secret_column_exists(self, db):
        """variables table must have a 'secret' column after migration."""
        with db._connect() as conn:
            cols = {r[1] for r in conn.execute("PRAGMA table_info(variables)").fetchall()}
        assert "secret" in cols

    def test_set_secret_encrypts_in_db(self, db):
        """Stored raw value must NOT be the plaintext when secret=True."""
        db.variable_set("my_api_key", "super-secret-value", secret=True)
        with db._connect() as conn:
            row = conn.execute(
                "SELECT value, secret FROM variables WHERE name=?", ("my_api_key",)
            ).fetchone()
        assert row is not None
        raw_value, is_secret = row
        assert is_secret == 1
        # Raw stored value must be different from plaintext (it's Fernet-encrypted)
        assert raw_value != "super-secret-value"
        assert len(raw_value) > 20  # ciphertext is much longer

    def test_set_non_secret_stores_plaintext(self, db):
        """Non-secret variable is stored as plaintext."""
        db.variable_set("plain_var", "hello-world", secret=False)
        with db._connect() as conn:
            row = conn.execute(
                "SELECT value, secret FROM variables WHERE name=?", ("plain_var",)
            ).fetchone()
        raw_value, is_secret = row
        assert is_secret == 0
        assert raw_value == "hello-world"

    def test_variable_get_decrypts_secret(self, db):
        """variable_get must return plaintext for secret variables."""
        db.variable_set("db_password", "p4ssw0rd!", secret=True)
        result = db.variable_get("db_password")
        assert result == "p4ssw0rd!"

    def test_variable_get_non_secret_unchanged(self, db):
        """variable_get for non-secret must return value unchanged."""
        db.variable_set("base_url", "https://example.com", secret=False)
        assert db.variable_get("base_url") == "https://example.com"

    def test_variable_list_hides_secret_value(self, db):
        """variable_list must show '***SECRET***' for secret variables."""
        db.variable_set("token", "my-token-123", secret=True)
        db.variable_set("url", "https://api.example.com", secret=False)
        rows = db.variable_list()
        secret_row = next(r for r in rows if r["name"] == "token")
        plain_row = next(r for r in rows if r["name"] == "url")
        assert secret_row["value"] == "***SECRET***"
        assert secret_row["secret"] is True
        assert plain_row["value"] == "https://api.example.com"
        assert plain_row["secret"] is False

    def test_variable_get_raw_returns_encrypted(self, db):
        """variable_get_raw must return the raw encrypted value and secret flag."""
        db.variable_set("raw_secret", "mysecret", secret=True)
        raw = db.variable_get_raw("raw_secret")
        assert raw is not None
        assert raw["secret"] in (1, True)
        assert raw["value"] != "mysecret"

    def test_variable_get_raw_nonexistent(self, db):
        """variable_get_raw returns None for nonexistent variable."""
        assert db.variable_get_raw("does_not_exist") is None

    def test_update_secret_variable(self, db):
        """Updating a secret variable re-encrypts the new value."""
        db.variable_set("rotating_key", "old-value", secret=True)
        db.variable_set("rotating_key", "new-value", secret=True)
        assert db.variable_get("rotating_key") == "new-value"

    def test_secret_defaults_false(self, db):
        """variable_set without secret parameter defaults to non-secret."""
        db.variable_set("default_var", "plaintext")
        rows = db.variable_list()
        row = next(r for r in rows if r["name"] == "default_var")
        assert row["secret"] is False
        assert row["value"] == "plaintext"


# ===========================================================================
# Part 2: MCP Handlers
# ===========================================================================

class TestSecretVariableMCPHandlers:
    def test_set_variable_secret_true(self, tmp_path, monkeypatch):
        """MCP set_variable with secret=True should encrypt value."""
        test_db_path = tmp_path / "brix_mcp_secret.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(BrixDB, "__init__",
                            lambda self, db_path=None: original_init(self, db_path=test_db_path))

        from brix.mcp_handlers.variables import _handle_set_variable
        result = asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "secret_token", "value": "tok123", "secret": True})
        )
        assert result["set"] is True
        assert result["secret"] is True

        db = BrixDB()
        # Value in DB should be encrypted
        with db._connect() as conn:
            row = conn.execute(
                "SELECT value, secret FROM variables WHERE name=?", ("secret_token",)
            ).fetchone()
        assert row[1] == 1
        assert row[0] != "tok123"

    def test_get_variable_secret_no_value(self, tmp_path, monkeypatch):
        """MCP get_variable for secret variable must NOT return value."""
        test_db_path = tmp_path / "brix_mcp_get_secret.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(BrixDB, "__init__",
                            lambda self, db_path=None: original_init(self, db_path=test_db_path))

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_get_variable
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "hidden", "value": "s3cr3t", "secret": True})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_variable({"name": "hidden"})
        )
        assert result["found"] is True
        assert result["secret"] is True
        assert result["value"] == ""  # No value exposed
        assert "note" in result

    def test_get_variable_non_secret_returns_value(self, tmp_path, monkeypatch):
        """MCP get_variable for non-secret returns value normally."""
        test_db_path = tmp_path / "brix_mcp_get_plain.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(BrixDB, "__init__",
                            lambda self, db_path=None: original_init(self, db_path=test_db_path))

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_get_variable
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "plain", "value": "visible"})
        )
        result = asyncio.get_event_loop().run_until_complete(
            _handle_get_variable({"name": "plain"})
        )
        assert result["found"] is True
        assert result["value"] == "visible"
        assert result.get("secret") is False

    def test_list_variables_secret_masked(self, tmp_path, monkeypatch):
        """MCP list_variables must mask secret values as '***SECRET***'."""
        test_db_path = tmp_path / "brix_mcp_list_secret.db"
        original_init = BrixDB.__init__
        monkeypatch.setattr(BrixDB, "__init__",
                            lambda self, db_path=None: original_init(self, db_path=test_db_path))

        from brix.mcp_handlers.variables import _handle_set_variable, _handle_list_variables
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "sec_var", "value": "hidden123", "secret": True})
        )
        asyncio.get_event_loop().run_until_complete(
            _handle_set_variable({"name": "pub_var", "value": "public_val", "secret": False})
        )
        result = asyncio.get_event_loop().run_until_complete(_handle_list_variables({}))
        assert result["count"] == 2
        sec = next(v for v in result["variables"] if v["name"] == "sec_var")
        pub = next(v for v in result["variables"] if v["name"] == "pub_var")
        assert sec["value"] == "***SECRET***"
        assert sec["secret"] is True
        assert pub["value"] == "public_val"
        assert pub["secret"] is False


# ===========================================================================
# Part 3: Jinja2 Context
# ===========================================================================

class TestSecretVariableJinja2:
    def test_secret_var_decrypted_in_jinja_context(self, patched_db, tmp_path):
        """{{ var.secret_name }} must yield the decrypted plaintext."""
        patched_db.variable_set("jinja_secret", "runtime-value", secret=True)
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert jctx["var"].get("jinja_secret") == "runtime-value"

    def test_non_secret_var_unchanged_in_jinja_context(self, patched_db, tmp_path):
        """Non-secret {{ var.name }} must yield the plaintext unchanged."""
        patched_db.variable_set("plain_jinja", "plain-value", secret=False)
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert jctx["var"].get("plain_jinja") == "plain-value"

    def test_secret_values_set_populated(self, patched_db, tmp_path):
        """_secret_values must contain plaintext of secret variables."""
        patched_db.variable_set("tracked_secret", "track-me!", secret=True)
        ctx = PipelineContext(workdir=tmp_path / "run")
        ctx.to_jinja_context()  # trigger cache build
        assert "track-me!" in ctx._secret_values

    def test_non_secret_not_in_secret_values(self, patched_db, tmp_path):
        """Non-secret variable values must NOT appear in _secret_values."""
        patched_db.variable_set("public_key", "public123", secret=False)
        ctx = PipelineContext(workdir=tmp_path / "run")
        ctx.to_jinja_context()
        assert "public123" not in ctx._secret_values

    def test_jinja_context_secret_values_key_present(self, patched_db, tmp_path):
        """_secret_values must be present in the Jinja2 context dict."""
        ctx = PipelineContext(workdir=tmp_path / "run")
        jctx = ctx.to_jinja_context()
        assert "_secret_values" in jctx
        assert isinstance(jctx["_secret_values"], set)


# ===========================================================================
# Part 4: Redaction in step_executions
# ===========================================================================

class TestRedactSecretValues:
    def test_redact_replaces_secret_in_dict(self):
        """_redact_secret_values must replace secret strings in nested dicts."""
        from brix.engine import _redact_secret_values
        data = {"url": "https://api.example.com", "token": "super-secret-token"}
        result = _redact_secret_values(data, {"super-secret-token"})
        assert result["token"] == "***REDACTED***"
        assert result["url"] == "https://api.example.com"

    def test_redact_replaces_secret_in_nested_structure(self):
        """Redaction must work in nested structures."""
        from brix.engine import _redact_secret_values
        data = {"outer": {"inner": "my-secret-value"}, "list": ["my-secret-value", "safe"]}
        result = _redact_secret_values(data, {"my-secret-value"})
        assert result["outer"]["inner"] == "***REDACTED***"
        assert result["list"][0] == "***REDACTED***"
        assert result["list"][1] == "safe"

    def test_redact_empty_secrets_returns_unchanged(self):
        """Empty secret_values set → data returned unchanged."""
        from brix.engine import _redact_secret_values
        data = {"key": "some-value"}
        result = _redact_secret_values(data, set())
        assert result == {"key": "some-value"}

    def test_redact_none_data(self):
        """None data → None returned."""
        from brix.engine import _redact_secret_values
        result = _redact_secret_values(None, {"secret"})
        assert result is None

    def test_redact_non_serializable_returns_original(self):
        """Non-JSON-serializable data → original returned (best-effort)."""
        from brix.engine import _redact_secret_values

        class Unserializable:
            pass

        data = Unserializable()
        result = _redact_secret_values(data, {"secret"})
        assert result is data

    def test_redact_multiple_secrets(self):
        """Multiple secrets are all redacted."""
        from brix.engine import _redact_secret_values
        data = {"a": "alpha-secret", "b": "beta-secret", "c": "safe"}
        result = _redact_secret_values(data, {"alpha-secret", "beta-secret"})
        assert result["a"] == "***REDACTED***"
        assert result["b"] == "***REDACTED***"
        assert result["c"] == "safe"
