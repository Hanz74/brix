"""Tests for T-BRIX-V8-09: Schema-Consultation Tracking + Typ-Kompatibilitätsprüfung.

Covers:
1. add_step ohne vorheriges get_brick_schema → Warnung
2. add_step nach get_brick_schema → keine Warnung
3. create_pipeline mit inkompatiblen Typen → Warnung
4. create_pipeline mit kompatiblen Typen → keine Warnung
5. get_brick_schema trackt Consultation
6. TTL: alte Consultations werden ignoriert
7. get_brick_schema zeigt compatible_with
"""
from __future__ import annotations

import asyncio
import time
import pytest
from unittest.mock import patch, MagicMock
from pathlib import Path


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _run(coro):
    return asyncio.get_event_loop().run_until_complete(coro)


# ---------------------------------------------------------------------------
# 1. Schema-Consultation Tracking (shared state)
# ---------------------------------------------------------------------------

class TestSchemaConsultationTracking:
    """Unit tests for record_schema_consultation / was_schema_consulted."""

    def setup_method(self):
        """Clear consultation state before each test."""
        from brix.mcp_handlers import _shared
        _shared._schema_consultations.clear()

    def test_record_and_check_consultation(self):
        """record_schema_consultation stores, was_schema_consulted returns True."""
        from brix.mcp_handlers._shared import record_schema_consultation, was_schema_consulted
        source = {"session": "test-session-1"}
        record_schema_consultation(source, "specialist")
        assert was_schema_consulted(source, "specialist") is True

    def test_not_consulted_returns_false(self):
        """was_schema_consulted returns False when brick was never consulted."""
        from brix.mcp_handlers._shared import was_schema_consulted
        source = {"session": "new-session"}
        assert was_schema_consulted(source, "specialist") is False

    def test_different_sources_isolated(self):
        """Consultations for one source do not bleed into another."""
        from brix.mcp_handlers._shared import record_schema_consultation, was_schema_consulted
        source_a = {"session": "session-alpha"}
        source_b = {"session": "session-beta"}
        record_schema_consultation(source_a, "filter")
        assert was_schema_consulted(source_a, "filter") is True
        assert was_schema_consulted(source_b, "filter") is False

    def test_none_source_uses_global_key(self):
        """None source uses __global__ key — consultations are shared."""
        from brix.mcp_handlers._shared import record_schema_consultation, was_schema_consulted
        record_schema_consultation(None, "transform")
        assert was_schema_consulted(None, "transform") is True

    def test_ttl_expired_returns_false(self):
        """Entries older than TTL are treated as expired."""
        from brix.mcp_handlers import _shared
        from brix.mcp_handlers._shared import was_schema_consulted

        source = {"session": "ttl-test"}
        key = "ttl-test-session"
        # Directly insert with an ancient timestamp (1 hour ago)
        _shared._schema_consultations["session:ttl-test"] = {
            "specialist": time.time() - 3700  # older than 30 min TTL
        }
        assert was_schema_consulted(source, "specialist") is False

    def test_fresh_entry_not_expired(self):
        """Entries within TTL are still valid."""
        from brix.mcp_handlers import _shared
        from brix.mcp_handlers._shared import was_schema_consulted

        source = {"session": "fresh-test"}
        _shared._schema_consultations["session:fresh-test"] = {
            "specialist": time.time() - 60  # 1 minute ago — within TTL
        }
        assert was_schema_consulted(source, "specialist") is True


# ---------------------------------------------------------------------------
# 2. get_brick_schema — tracks consultation + compatible_with
# ---------------------------------------------------------------------------

class TestGetBrickSchemaTracking:
    """Tests that get_brick_schema tracks consultation and returns compatible_with."""

    def setup_method(self):
        from brix.mcp_handlers import _shared
        _shared._schema_consultations.clear()

    def test_get_brick_schema_records_consultation(self, tmp_path, monkeypatch):
        """Calling get_brick_schema should record a consultation entry."""
        from brix.mcp_handlers._shared import was_schema_consulted
        from brix.mcp_handlers.steps import _handle_get_brick_schema

        source = {"session": "gbs-session"}
        result = _run(_handle_get_brick_schema({"brick_name": "specialist", "source": source}))

        assert result.get("name") == "specialist"
        assert was_schema_consulted(source, "specialist") is True

    def test_get_brick_schema_returns_compatible_with(self, tmp_path, monkeypatch):
        """get_brick_schema returns compatible_with list when brick has input_type."""
        from brix.mcp_handlers.steps import _handle_get_brick_schema

        source = {"session": "compat-session"}
        # llm.extract has input_type="string (text)" — other bricks with output_type=string
        # or compatible should appear in compatible_with
        result = _run(_handle_get_brick_schema({"brick_name": "llm.extract", "source": source}))

        assert "compatible_with" in result
        assert isinstance(result["compatible_with"], list)

    def test_get_brick_schema_compatible_with_empty_when_no_input_type(self):
        """Bricks without input_type return empty compatible_with list."""
        from brix.mcp_handlers.steps import _handle_get_brick_schema

        # specialist has input_type set, but let's test a brick with no input_type
        # http_get has no input_type
        result = _run(_handle_get_brick_schema({"brick_name": "http_get", "source": None}))
        assert result.get("compatible_with") == []

    def test_get_brick_schema_not_found_returns_error(self):
        """Non-existent brick returns error dict."""
        from brix.mcp_handlers.steps import _handle_get_brick_schema
        result = _run(_handle_get_brick_schema({"brick_name": "nonexistent_brick_xyz"}))
        assert result.get("success") is False
        assert "not found" in result.get("error", "")


# ---------------------------------------------------------------------------
# 3. add_step — schema consultation warning
# ---------------------------------------------------------------------------

class TestAddStepSchemaConsultationWarning:
    """Tests that add_step warns when get_brick_schema was not called first."""

    def setup_method(self):
        from brix.mcp_handlers import _shared
        _shared._schema_consultations.clear()

    def _make_pipeline(self, tmp_path, name="test-pipe"):
        import yaml
        data = {"name": name, "version": "1.0.0", "steps": []}
        (tmp_path / f"{name}.yaml").write_text(yaml.dump(data))

    def test_add_step_without_schema_consultation_warns(self, tmp_path, monkeypatch):
        """add_step warns if get_brick_schema was not called for the brick."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)
        self._make_pipeline(tmp_path)

        from brix.mcp_handlers.steps import _handle_add_step
        source = {"session": "warn-session"}

        result = _run(_handle_add_step({
            "pipeline_name": "test-pipe",
            "step_id": "step1",
            "brick": "specialist",
            "source": source,
        }))

        assert result["success"] is True
        # Should have a warning about missing schema consultation
        warning_text = result.get("warning", "") or " ".join(result.get("warnings", []))
        assert "get_brick_schema" in warning_text
        assert "specialist" in warning_text

    def test_add_step_after_schema_consultation_no_warning(self, tmp_path, monkeypatch):
        """add_step does NOT warn if get_brick_schema was called beforehand."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)
        self._make_pipeline(tmp_path)

        from brix.mcp_handlers.steps import _handle_add_step, _handle_get_brick_schema
        source = {"session": "no-warn-session"}

        # First: consult the schema
        _run(_handle_get_brick_schema({"brick_name": "specialist", "source": source}))

        # Then: add the step
        result = _run(_handle_add_step({
            "pipeline_name": "test-pipe",
            "step_id": "step1",
            "brick": "specialist",
            "source": source,
        }))

        assert result["success"] is True
        # No consultation warning
        warning_text = result.get("warning", "") or " ".join(result.get("warnings", []))
        assert "get_brick_schema" not in warning_text

    def test_add_step_with_type_no_schema_warning(self, tmp_path, monkeypatch):
        """add_step with 'type' (not 'brick') does NOT produce a schema warning."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)
        self._make_pipeline(tmp_path)

        from brix.mcp_handlers.steps import _handle_add_step
        source = {"session": "type-session"}

        result = _run(_handle_add_step({
            "pipeline_name": "test-pipe",
            "step_id": "step1",
            "type": "repeat",
            "source": source,
        }))

        assert result["success"] is True
        warning_text = result.get("warning", "") or " ".join(result.get("warnings", []))
        assert "get_brick_schema" not in warning_text


# ---------------------------------------------------------------------------
# 4. add_step — type compatibility check
# ---------------------------------------------------------------------------

class TestAddStepTypeCompatibility:
    """Tests that add_step warns on type incompatibility with adjacent steps."""

    def setup_method(self):
        from brix.mcp_handlers import _shared
        _shared._schema_consultations.clear()

    def _make_pipeline_with_steps(self, tmp_path, name, steps):
        import yaml
        data = {"name": name, "version": "1.0.0", "steps": steps}
        (tmp_path / f"{name}.yaml").write_text(yaml.dump(data))

    def test_add_step_incompatible_with_previous_warns(self, tmp_path, monkeypatch):
        """add_step warns when new step's input_type is incompatible with previous step output_type."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        # source.fetch_emails → output_type="list[email]"
        # db.ingest → input_type="list[object] | object"
        # These are incompatible: list[email] is not list[object]
        # (list[email] → list[dict] is compatible, but list[email] → "list[object] | object" is not)
        # Use convert.to_markdown (input_type="file_path") after source.fetch_emails (output_type="list[email]")
        # file_path ≠ list[email] → should warn
        self._make_pipeline_with_steps(tmp_path, "pipe-compat", [
            {"id": "fetch", "type": "specialist", "brick": "source.fetch_emails"},
        ])
        from brix.mcp_handlers.steps import _handle_add_step, _handle_get_brick_schema
        source = {"session": "compat-add-session"}
        # Consult schema to suppress consultation warning
        _run(_handle_get_brick_schema({"brick_name": "convert.to_markdown", "source": source}))

        # convert.to_markdown expects input_type="file_path"
        # But previous step (source.fetch_emails) outputs "list[email]"
        result = _run(_handle_add_step({
            "pipeline_name": "pipe-compat",
            "step_id": "convert",
            "brick": "convert.to_markdown",
            "source": source,
        }))

        assert result["success"] is True
        all_warnings = result.get("warning", "") + " ".join(result.get("warnings", []))
        assert "TYP-INKOMPATIBILITÄT" in all_warnings

    def test_add_step_compatible_with_previous_no_type_warning(self, tmp_path, monkeypatch):
        """add_step with compatible types does not generate a type warning."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        # source.fetch_emails → output_type="list[email]"
        # db.ingest → input_type="list[object] | object"
        # list[email] is a specialisation of list[*] / list[dict].
        # But db.ingest uses "list[object] | object" which is not in compatibility table for list[email].
        # Use a definitely-compatible pair instead:
        # convert.to_markdown → output_type="string (markdown)"
        # "string (markdown)" → ["string (markdown)", "string", "text", "markdown"]
        # So a step that accepts "string" or "text" is compatible.
        # However neither llm.extract (string (text)) nor llm.classify (string (text)) is listed.
        # Use two bricks that are untyped (no input/output type) — should produce no type warning.
        self._make_pipeline_with_steps(tmp_path, "pipe-ok", [
            {"id": "fetch", "type": "http", "brick": "http_get"},
        ])
        from brix.mcp_handlers.steps import _handle_add_step, _handle_get_brick_schema
        source = {"session": "compat-ok-session"}
        _run(_handle_get_brick_schema({"brick_name": "http_post", "source": source}))

        # http_get and http_post have no input_type/output_type → no type warning
        result = _run(_handle_add_step({
            "pipeline_name": "pipe-ok",
            "step_id": "post_step",
            "brick": "http_post",
            "source": source,
        }))

        assert result["success"] is True
        all_warnings = result.get("warning", "") + " ".join(result.get("warnings", []))
        assert "TYP-INKOMPATIBILITÄT" not in all_warnings


# ---------------------------------------------------------------------------
# 5. create_pipeline — type compatibility check
# ---------------------------------------------------------------------------

class TestCreatePipelineTypeCompatibility:
    """Tests that create_pipeline warns on incompatible step type pairs."""

    def setup_method(self):
        from brix.mcp_handlers import _shared
        _shared._schema_consultations.clear()

    def test_create_pipeline_incompatible_steps_warns(self, tmp_path, monkeypatch):
        """create_pipeline warns when adjacent steps have incompatible types."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        # source.fetch_emails → output_type="list[email]"
        # convert.to_markdown → input_type="file_path"
        # These are incompatible
        result = _run(_handle_create_pipeline({
            "name": "incompat-pipe",
            "steps": [
                {"id": "fetch", "brick": "source.fetch_emails", "type": "specialist"},
                {"id": "convert", "brick": "convert.to_markdown", "type": "specialist"},
            ],
        }))

        assert result["success"] is True
        warnings = result.get("warnings", [])
        warning_text = " ".join(warnings)
        assert "TYP-INKOMPATIBILITÄT" in warning_text

    def test_create_pipeline_compatible_steps_no_type_warning(self, tmp_path, monkeypatch):
        """create_pipeline with compatible step types has no type incompatibility warning."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        # http_get and filter have no typed input/output → no type incompatibility warning
        result = _run(_handle_create_pipeline({
            "name": "compat-pipe",
            "steps": [
                {"id": "fetch", "brick": "http_get", "type": "http"},
                {"id": "filter_step", "brick": "filter", "type": "filter"},
            ],
        }))

        assert result["success"] is True
        warnings = result.get("warnings", [])
        warning_text = " ".join(warnings)
        assert "TYP-INKOMPATIBILITÄT" not in warning_text

    def test_create_pipeline_no_steps_no_warning(self, tmp_path, monkeypatch):
        """create_pipeline with empty steps list has no warnings."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        result = _run(_handle_create_pipeline({"name": "empty-pipe", "steps": []}))
        assert result["success"] is True
        warning_text = " ".join(result.get("warnings", []))
        assert "TYP-INKOMPATIBILITÄT" not in warning_text

    def test_create_pipeline_single_step_no_type_warning(self, tmp_path, monkeypatch):
        """create_pipeline with a single step has no type-compatibility warning."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        result = _run(_handle_create_pipeline({
            "name": "single-step-pipe",
            "steps": [
                {"id": "fetch", "brick": "source.fetch_emails", "type": "specialist"},
            ],
        }))
        assert result["success"] is True
        warning_text = " ".join(result.get("warnings", []))
        assert "TYP-INKOMPATIBILITÄT" not in warning_text

    def test_create_pipeline_unknown_brick_skipped_silently(self, tmp_path, monkeypatch):
        """Unknown brick names are silently skipped in type-compatibility checks."""
        import brix.mcp_server as mcp_mod
        monkeypatch.setattr(mcp_mod, "PIPELINE_DIR", tmp_path)

        from brix.mcp_handlers.pipelines import _handle_create_pipeline

        result = _run(_handle_create_pipeline({
            "name": "unknown-brick-pipe",
            "steps": [
                {"id": "s1", "brick": "nonexistent_brick_a", "type": "python"},
                {"id": "s2", "brick": "nonexistent_brick_b", "type": "python"},
            ],
        }))
        assert result["success"] is True
        # No TYP-INKOMPATIBILITÄT warning for unknown bricks
        warning_text = " ".join(result.get("warnings", []))
        assert "TYP-INKOMPATIBILITÄT" not in warning_text
