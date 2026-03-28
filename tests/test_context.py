"""Tests for brix.context.PipelineContext."""

import os

import pytest

from brix.context import PipelineContext
from brix.loader import PipelineLoader


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

SIMPLE_PIPELINE_YAML = """
name: test-pipeline
input:
  greeting:
    type: string
    default: hello
  count:
    type: integer
    default: 3
credentials:
  api_key:
    env: TEST_API_KEY
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
"""


def _load_pipeline(yaml_str: str):
    loader = PipelineLoader()
    return loader.load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------


def test_context_creation():
    """PipelineContext can be instantiated with explicit input/credentials."""
    ctx = PipelineContext(
        pipeline_input={"foo": "bar"},
        credentials={"key": "secret"},
    )
    assert ctx.input == {"foo": "bar"}
    assert ctx.credentials == {"key": "secret"}
    assert ctx.step_outputs == {}
    assert ctx.run_id.startswith("run-")
    assert len(ctx.run_id) == len("run-") + 12


def test_context_run_id_unique():
    """Each context gets a unique run_id."""
    ctx1 = PipelineContext()
    ctx2 = PipelineContext()
    assert ctx1.run_id != ctx2.run_id


def test_context_from_pipeline_with_defaults():
    """from_pipeline resolves defaults from pipeline input definition."""
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.input["greeting"] == "hello"
    assert ctx.input["count"] == 3


def test_context_from_pipeline_with_user_input():
    """User input overrides pipeline defaults."""
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline, user_input={"greeting": "world"})
    assert ctx.input["greeting"] == "world"
    assert ctx.input["count"] == 3  # still default


def test_context_credentials_from_env(monkeypatch):
    """Credentials are resolved from environment variables."""
    monkeypatch.setenv("TEST_API_KEY", "my-secret-key")
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.credentials["api_key"] == "my-secret-key"


def test_context_credentials_missing_env():
    """Missing env var results in empty string credential (not an error)."""
    # Ensure env var is not set
    os.environ.pop("TEST_API_KEY", None)
    pipeline = _load_pipeline(SIMPLE_PIPELINE_YAML)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.credentials["api_key"] == ""


def test_context_set_get_output():
    """set_output stores value, get_output retrieves it."""
    ctx = PipelineContext()
    ctx.set_output("step_a", {"result": 42})
    assert ctx.get_output("step_a") == {"result": 42}


def test_context_get_output_missing_returns_none():
    """get_output returns None for unknown step IDs."""
    ctx = PipelineContext()
    assert ctx.get_output("nonexistent") is None


def test_context_to_jinja_context():
    """to_jinja_context returns correct structure with input, credentials, step outputs."""
    ctx = PipelineContext(
        pipeline_input={"name": "alice"},
        credentials={"token": "abc123"},
    )
    ctx.set_output("fetch", ["item1", "item2"])
    ctx.set_output("transform", {"count": 2})

    jinja_ctx = ctx.to_jinja_context()

    assert jinja_ctx["input"] == {"name": "alice"}
    assert jinja_ctx["credentials"] == {"token": "abc123"}
    assert jinja_ctx["fetch"] == {"output": ["item1", "item2"]}
    assert jinja_ctx["transform"] == {"output": {"count": 2}}
    assert "item" not in jinja_ctx


def test_context_to_jinja_context_with_item():
    """to_jinja_context includes 'item' key when item is provided."""
    ctx = PipelineContext(pipeline_input={"x": 1})
    jinja_ctx = ctx.to_jinja_context(item="current-item")
    assert jinja_ctx["item"] == "current-item"


def test_context_to_jinja_context_item_none_excluded():
    """item=None should NOT add 'item' key to the context."""
    ctx = PipelineContext()
    jinja_ctx = ctx.to_jinja_context(item=None)
    assert "item" not in jinja_ctx


# ---------------------------------------------------------------------------
# Large-output / JSONL tests (T-BRIX-V3-02)
# ---------------------------------------------------------------------------


def test_set_output_small_stays_in_ram():
    """Outputs with <=LARGE_OUTPUT_THRESHOLD items stay as plain dict in RAM."""
    from brix.context import LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    items = [{"i": i} for i in range(LARGE_OUTPUT_THRESHOLD)]  # exactly at threshold — not over
    output = {"items": items, "summary": {"total": len(items)}}
    ctx.set_output("step1", output)

    stored = ctx.step_outputs["step1"]
    # Should be the original dict, NOT a _large_output reference
    assert "_large_output" not in stored
    assert stored["items"] == items


def test_set_output_large_by_size_goes_to_jsonl():
    """Outputs whose serialized JSON exceeds LARGE_OUTPUT_SIZE_BYTES spill to JSONL,
    even when the item count is below LARGE_OUTPUT_THRESHOLD (INBOX-347)."""
    from brix.context import LARGE_OUTPUT_SIZE_BYTES, LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    # Craft a small number of items (below count threshold) but each item is large
    # enough that the total exceeds 10 MB.
    item_count = min(LARGE_OUTPUT_THRESHOLD, 5)  # well below count threshold
    # Each item needs to push total > LARGE_OUTPUT_SIZE_BYTES
    payload_per_item = (LARGE_OUTPUT_SIZE_BYTES // item_count) + 1
    items = [{"data": "x" * payload_per_item, "idx": i} for i in range(item_count)]
    output = {"items": items, "summary": {"total": item_count}}
    ctx.set_output("heavy_step", output)

    stored = ctx.step_outputs["heavy_step"]
    assert stored.get("_large_output") is True, "Large-by-size output should be spilled to JSONL"
    assert stored["_count"] == item_count
    assert stored["summary"] == output["summary"]

    # JSONL file must contain all items
    import json as _json
    lines = [l for l in open(stored["_jsonl_path"]) if l.strip()]
    assert len(lines) == item_count


def test_get_output_large_by_size_reconstructs():
    """get_output transparently loads items spilled by the size threshold."""
    from brix.context import LARGE_OUTPUT_SIZE_BYTES, LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    item_count = min(LARGE_OUTPUT_THRESHOLD, 3)
    payload_per_item = (LARGE_OUTPUT_SIZE_BYTES // item_count) + 1
    items = [{"data": "y" * payload_per_item, "idx": i} for i in range(item_count)]
    summary = {"total": item_count}
    ctx.set_output("size_step", {"items": items, "summary": summary})

    result = ctx.get_output("size_step")
    assert result is not None
    assert len(result["items"]) == item_count
    assert result["items"][0]["idx"] == 0
    assert result["summary"] == summary


def test_jinja_context_large_by_size_summary_only():
    """to_jinja_context exposes only summary + count for size-spilled outputs."""
    from brix.context import LARGE_OUTPUT_SIZE_BYTES, LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    item_count = min(LARGE_OUTPUT_THRESHOLD, 3)
    payload_per_item = (LARGE_OUTPUT_SIZE_BYTES // item_count) + 1
    items = [{"data": "z" * payload_per_item} for _ in range(item_count)]
    summary = {"total": item_count, "succeeded": item_count}
    ctx.set_output("size_heavy", {"items": items, "summary": summary})

    jinja_ctx = ctx.to_jinja_context()
    step_output = jinja_ctx["size_heavy"]["output"]

    assert "items" not in step_output
    assert step_output["summary"] == summary
    assert step_output["count"] == item_count
    assert step_output["_large"] is True


def test_set_output_large_goes_to_jsonl():
    """Outputs with >LARGE_OUTPUT_THRESHOLD items are written to a JSONL file."""
    from brix.context import LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    items = [{"i": i} for i in range(LARGE_OUTPUT_THRESHOLD + 1)]
    output = {"items": items, "summary": {"total": len(items), "succeeded": len(items)}}
    ctx.set_output("big_step", output)

    stored = ctx.step_outputs["big_step"]
    # RAM entry must be the lightweight reference
    assert stored.get("_large_output") is True
    assert stored["_count"] == len(items)
    assert stored["summary"] == output["summary"]

    # JSONL file must exist and contain all items
    import json as _json
    jsonl_path = stored["_jsonl_path"]
    lines = [l for l in open(jsonl_path) if l.strip()]
    assert len(lines) == len(items)
    first = _json.loads(lines[0])
    assert first == {"i": 0}


def test_get_output_loads_from_jsonl():
    """get_output transparently reconstructs the full output from the JSONL file."""
    from brix.context import LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    items = [{"val": v} for v in range(LARGE_OUTPUT_THRESHOLD + 5)]
    summary = {"total": len(items), "succeeded": len(items), "failed": 0}
    ctx.set_output("loader_step", {"items": items, "summary": summary})

    # get_output must return the full reconstructed dict
    result = ctx.get_output("loader_step")
    assert result is not None
    assert result["items"] == items
    assert result["summary"] == summary


def test_jinja_context_large_output_summary_only():
    """to_jinja_context exposes only summary + count for large outputs, not the items list."""
    from brix.context import LARGE_OUTPUT_THRESHOLD

    ctx = PipelineContext()
    items = [{"x": i} for i in range(LARGE_OUTPUT_THRESHOLD + 1)]
    summary = {"total": len(items), "succeeded": len(items), "failed": 0}
    ctx.set_output("heavy", {"items": items, "summary": summary})

    jinja_ctx = ctx.to_jinja_context()
    step_output = jinja_ctx["heavy"]["output"]

    # Must NOT contain the raw items list
    assert "items" not in step_output
    # Must contain summary and count
    assert step_output["summary"] == summary
    assert step_output["count"] == len(items)
    assert step_output["_large"] is True


# ---------------------------------------------------------------------------
# Jinja2 context cache tests (T-BRIX-V3-18)
# ---------------------------------------------------------------------------


def test_jinja_context_cached():
    """Second call to to_jinja_context returns the same object (id() equal)."""
    ctx = PipelineContext(pipeline_input={"x": 1})
    ctx.set_output("step1", {"result": "ok"})

    first = ctx.to_jinja_context()
    second = ctx.to_jinja_context()

    assert first is second, "Cache miss: to_jinja_context returned a new object on second call"


def test_jinja_context_invalidated_on_set_output():
    """set_output invalidates the cache — next call rebuilds the context."""
    ctx = PipelineContext(pipeline_input={"x": 1})
    ctx.set_output("step1", {"result": "ok"})

    first = ctx.to_jinja_context()
    assert "step1" in first

    ctx.set_output("step2", {"result": "new"})
    second = ctx.to_jinja_context()

    # Must be a different object (cache was invalidated)
    assert first is not second
    # New context must contain step2
    assert "step2" in second
    assert second["step2"] == {"output": {"result": "new"}}


def test_jinja_context_item_does_not_pollute_cache():
    """Calling to_jinja_context(item=...) does not store item in the cache."""
    ctx = PipelineContext(pipeline_input={"x": 1})

    # Call with an item
    with_item = ctx.to_jinja_context(item="my-item")
    assert with_item["item"] == "my-item"

    # Subsequent call without item should not contain "item"
    without_item = ctx.to_jinja_context()
    assert "item" not in without_item

    # The cached object itself must not have "item"
    assert "item" not in ctx._jinja_cache  # type: ignore[union-attr]


def test_history_no_items_array(tmp_path):
    """record_finish receives only status/duration/items-count/errors — no raw items arrays."""
    import json as _json
    from brix.history import RunHistory

    db = RunHistory(db_path=tmp_path / "test.db")
    db.record_start("run-test-001", "my-pipeline", "1.0", {"key": "val"})

    steps_summary = {
        "step1": {"status": "ok", "duration": 0.5, "items": 500, "errors": None},
        "step2": {"status": "ok", "duration": 1.2, "items": None, "errors": None},
    }
    db.record_finish("run-test-001", True, 1.7, steps_summary, {"result_type": "dict"})

    run = db.get_run("run-test-001")
    assert run is not None
    stored_steps = _json.loads(run["steps_data"])
    # Verify compact structure — no "data" or raw "items" arrays
    assert stored_steps["step1"]["status"] == "ok"
    assert stored_steps["step1"]["items"] == 500
    assert "data" not in stored_steps["step1"]
    assert stored_steps["step2"]["status"] == "ok"


# ---------------------------------------------------------------------------
# Credential refresh tests (T-BRIX-V4-13)
# ---------------------------------------------------------------------------


def test_credential_ref_with_refresh():
    """CredentialRef accepts an optional refresh dict."""
    from brix.models import CredentialRef

    cred = CredentialRef(env="MY_TOKEN", refresh={"type": "oauth2_client_credentials", "token_url": "https://example.com/token"})
    assert cred.env == "MY_TOKEN"
    assert cred.refresh["type"] == "oauth2_client_credentials"


def test_credential_ref_without_refresh():
    """CredentialRef works without refresh (backwards compatible)."""
    from brix.models import CredentialRef

    cred = CredentialRef(env="MY_TOKEN")
    assert cred.refresh is None


def test_credential_refresh_not_jwt():
    """_refresh_credential with no refresh config returns the current value unchanged."""
    from brix.models import CredentialRef

    cred = CredentialRef(env="DUMMY_TOKEN")
    # No refresh configured — static method should return current value unchanged
    result = PipelineContext._refresh_credential(cred, "my-static-token")
    assert result == "my-static-token"


def test_credential_refresh_skipped_when_no_refresh():
    """from_pipeline does not call _refresh_credential when refresh is None."""
    pipeline_yaml = """
name: test-refresh
credentials:
  api_token:
    env: DUMMY_REFRESH_TOKEN
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
"""
    import os
    os.environ["DUMMY_REFRESH_TOKEN"] = "static-value"
    pipeline = _load_pipeline(pipeline_yaml)
    ctx = PipelineContext.from_pipeline(pipeline)
    assert ctx.credentials["api_token"] == "static-value"
    os.environ.pop("DUMMY_REFRESH_TOKEN", None)
