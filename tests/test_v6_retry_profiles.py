"""Tests for T-BRIX-V6-04 — Retry-Profiles.

Covers:
- RetryProfile model (models.py)
- Pipeline.retry_profiles field
- Step.retry_profile field
- HTTP runner: status_code included in error responses
- Engine: _execute_with_retry with profile resolution
"""

import pytest
import httpx

from brix.engine import PipelineEngine
from brix.loader import PipelineLoader
from brix.models import Pipeline, RetryProfile, Step
from brix.runners.base import BaseRunner, _StubRunnerMixin
from brix.runners.http import HttpRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------


def load_pipeline(yaml_str: str) -> Pipeline:
    return PipelineLoader().load_from_string(yaml_str)


# ---------------------------------------------------------------------------
# RetryProfile model tests
# ---------------------------------------------------------------------------


def test_retry_profile_defaults():
    """RetryProfile has sensible defaults."""
    p = RetryProfile()
    assert p.max == 3
    assert p.backoff == "exponential"
    assert p.retriable_status_codes == []


def test_retry_profile_custom():
    """RetryProfile accepts all custom fields."""
    p = RetryProfile(max=5, backoff="linear", retriable_status_codes=[500, 502, 503])
    assert p.max == 5
    assert p.backoff == "linear"
    assert p.retriable_status_codes == [500, 502, 503]


def test_retry_profile_invalid_backoff():
    """Invalid backoff raises ValidationError."""
    from pydantic import ValidationError

    with pytest.raises(ValidationError):
        RetryProfile(backoff="random")


def test_retry_profile_empty_codes():
    """Empty retriable_status_codes means retry on all failures."""
    p = RetryProfile(retriable_status_codes=[])
    assert p.retriable_status_codes == []


# ---------------------------------------------------------------------------
# Pipeline.retry_profiles field tests
# ---------------------------------------------------------------------------


def test_pipeline_retry_profiles_default_empty():
    """Pipeline.retry_profiles defaults to empty dict."""
    step = Step(id="s1", type="python", script="run.py")
    p = Pipeline(name="test", steps=[step])
    assert p.retry_profiles == {}


def test_pipeline_retry_profiles_stored():
    """Pipeline.retry_profiles stores named RetryProfile entries."""
    step = Step(id="s1", type="python", script="run.py")
    profiles = {
        "transient": RetryProfile(max=3, retriable_status_codes=[500, 503]),
        "aggressive": RetryProfile(max=5, backoff="linear"),
    }
    p = Pipeline(name="test", steps=[step], retry_profiles=profiles)
    assert "transient" in p.retry_profiles
    assert "aggressive" in p.retry_profiles
    assert p.retry_profiles["transient"].retriable_status_codes == [500, 503]
    assert p.retry_profiles["aggressive"].max == 5


def test_pipeline_retry_profiles_yaml_roundtrip():
    """Pipeline with retry_profiles round-trips through YAML loading."""
    pipeline = load_pipeline("""
name: profile-roundtrip
retry_profiles:
  transient:
    max: 4
    backoff: linear
    retriable_status_codes: [500, 502, 503]
steps:
  - id: s1
    type: python
    script: run.py
""")
    assert "transient" in pipeline.retry_profiles
    prof = pipeline.retry_profiles["transient"]
    assert prof.max == 4
    assert prof.backoff == "linear"
    assert prof.retriable_status_codes == [500, 502, 503]


def test_pipeline_retry_profiles_serialization():
    """Pipeline.retry_profiles survives model_dump / model_validate roundtrip."""
    step = Step(id="s1", type="python", script="run.py")
    profiles = {"retry500": RetryProfile(max=2, retriable_status_codes=[500])}
    p = Pipeline(name="rt", steps=[step], retry_profiles=profiles)
    data = p.model_dump()
    p2 = Pipeline.model_validate(data)
    assert "retry500" in p2.retry_profiles
    assert p2.retry_profiles["retry500"].retriable_status_codes == [500]


# ---------------------------------------------------------------------------
# Step.retry_profile field tests
# ---------------------------------------------------------------------------


def test_step_retry_profile_default_none():
    """Step.retry_profile defaults to None."""
    s = Step(id="s1", type="python", script="run.py")
    assert s.retry_profile is None


def test_step_retry_profile_set():
    """Step.retry_profile stores the profile name."""
    s = Step(id="s1", type="python", script="run.py", retry_profile="transient")
    assert s.retry_profile == "transient"


def test_step_retry_profile_yaml_roundtrip():
    """Step.retry_profile is parsed correctly from YAML."""
    pipeline = load_pipeline("""
name: step-profile-test
retry_profiles:
  transient:
    max: 3
    retriable_status_codes: [500]
steps:
  - id: call
    type: http
    url: https://example.com
    on_error: retry
    retry_profile: transient
""")
    assert pipeline.steps[0].retry_profile == "transient"
    assert pipeline.steps[0].on_error == "retry"


# ---------------------------------------------------------------------------
# HTTP runner: status_code in error responses
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_http_runner_4xx_includes_status_code(monkeypatch):
    """HTTP 4xx errors include status_code in result dict."""

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(404, text="Not Found", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    class _Step:
        url = "https://example.com/missing"
        method = "GET"
        headers = None
        body = None
        fetch_all_pages = False
        timeout = None
        params = None

    result = await HttpRunner().execute(_Step(), context=None)
    assert result["success"] is False
    assert result["status_code"] == 404


@pytest.mark.asyncio
async def test_http_runner_5xx_includes_status_code(monkeypatch):
    """HTTP 5xx errors include status_code in result dict."""

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(500, text="Server Error", request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    class _Step:
        url = "https://example.com/error"
        method = "GET"
        headers = None
        body = None
        fetch_all_pages = False
        timeout = None
        params = None

    result = await HttpRunner().execute(_Step(), context=None)
    assert result["success"] is False
    assert result["status_code"] == 500


@pytest.mark.asyncio
async def test_http_runner_rate_limited_includes_status_code(monkeypatch):
    """Rate-limited responses (429/503) include status_code in result dict."""

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(
            429,
            text="Too Many Requests",
            headers={"Retry-After": "5"},
            request=httpx.Request(method, url),
        )

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    class _Step:
        url = "https://api.example.com/resource"
        method = "GET"
        headers = None
        body = None
        fetch_all_pages = False
        timeout = None
        params = None

    result = await HttpRunner().execute(_Step(), context=None)
    assert result["success"] is False
    assert result["status_code"] == 429
    assert result["rate_limited"] is True


@pytest.mark.asyncio
async def test_http_runner_success_has_no_status_code(monkeypatch):
    """Successful HTTP responses do not include status_code (no extra noise)."""

    async def mock_request(self, method, url, **kwargs):
        return httpx.Response(200, json={"ok": True}, request=httpx.Request(method, url))

    monkeypatch.setattr(httpx.AsyncClient, "request", mock_request)

    class _Step:
        url = "https://example.com"
        method = "GET"
        headers = None
        body = None
        fetch_all_pages = False
        timeout = None
        params = None

    result = await HttpRunner().execute(_Step(), context=None)
    assert result["success"] is True
    assert "status_code" not in result


# ---------------------------------------------------------------------------
# Engine: retry profile resolution
# ---------------------------------------------------------------------------


async def test_engine_retry_profile_unknown_name():
    """Referencing a non-existent retry_profile name returns success=False immediately."""

    class _Runner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            return {"success": False, "error": "fail", "duration": 0.01}

    pipeline = load_pipeline("""
name: unknown-profile
retry_profiles: {}
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: does_not_exist
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _Runner())
    result = await engine.run(pipeline)

    assert result.success is False
    step_status = result.steps["call"]
    assert step_status.status == "error"
    assert "does_not_exist" in step_status.error_message


async def test_engine_retry_profile_retriable_codes_retries_matching():
    """Engine retries when response status_code is in retriable_status_codes."""
    call_count = 0

    class _FailThenSucceedRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"success": False, "error": "server error", "status_code": 500, "duration": 0.01}
            return {"success": True, "data": "recovered", "duration": 0.01}

    pipeline = load_pipeline("""
name: profile-retry-match
retry_profiles:
  server_errors:
    max: 3
    backoff: linear
    retriable_status_codes: [500, 502, 503]
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: server_errors
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailThenSucceedRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 2


async def test_engine_retry_profile_retriable_codes_stops_on_non_retriable():
    """Engine stops retrying immediately when status_code is NOT in retriable_status_codes."""
    call_count = 0

    class _NonRetriableRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            # 404 is not in retriable list [500, 503]
            return {"success": False, "error": "not found", "status_code": 404, "duration": 0.01}

    pipeline = load_pipeline("""
name: profile-retry-no-match
retry_profiles:
  server_errors:
    max: 5
    backoff: linear
    retriable_status_codes: [500, 503]
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: server_errors
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _NonRetriableRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    # Should stop after first attempt since 404 is not retriable
    assert call_count == 1


async def test_engine_retry_profile_empty_retriable_codes_retries_all():
    """Empty retriable_status_codes means all failures are retried (no filtering)."""
    call_count = 0

    class _FailTwiceRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count < 3:
                return {"success": False, "error": "any error", "status_code": 404, "duration": 0.01}
            return {"success": True, "data": "ok", "duration": 0.01}

    pipeline = load_pipeline("""
name: profile-empty-codes
retry_profiles:
  retry_all:
    max: 5
    backoff: linear
    retriable_status_codes: []
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: retry_all
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailTwiceRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 3


async def test_engine_retry_profile_no_status_code_in_result_retries():
    """When result has no status_code and retriable_codes is non-empty, retries happen (no code = not filtered)."""
    call_count = 0

    class _NoStatusCodeRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                # No status_code in result — network-level error
                return {"success": False, "error": "connection refused", "duration": 0.01}
            return {"success": True, "data": "ok", "duration": 0.01}

    pipeline = load_pipeline("""
name: profile-no-code
retry_profiles:
  server_errors:
    max: 3
    backoff: linear
    retriable_status_codes: [500, 503]
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: server_errors
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _NoStatusCodeRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 2


async def test_engine_retry_profile_uses_profile_max_not_global():
    """Profile max overrides the pipeline-level error_handling.retry.max."""
    call_count = 0

    class _AlwaysFailRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            return {"success": False, "error": "fail", "status_code": 500, "duration": 0.01}

    pipeline = load_pipeline("""
name: profile-max-override
error_handling:
  on_error: retry
  retry:
    max: 10
    backoff: linear
retry_profiles:
  limited:
    max: 2
    backoff: linear
    retriable_status_codes: [500]
steps:
  - id: call
    type: cli
    on_error: retry
    retry_profile: limited
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _AlwaysFailRunner())
    result = await engine.run(pipeline)

    assert result.success is False
    # Profile max=2 limits attempts, not global max=10
    assert call_count == 2


async def test_engine_retry_without_profile_uses_global_config():
    """on_error=retry without retry_profile uses pipeline-level retry config."""
    call_count = 0

    class _FailOnceRunner(_StubRunnerMixin, BaseRunner):
        async def execute(self, step, context):
            nonlocal call_count
            call_count += 1
            if call_count == 1:
                return {"success": False, "error": "transient", "duration": 0.01}
            return {"success": True, "data": "ok", "duration": 0.01}

    pipeline = load_pipeline("""
name: no-profile-fallback
error_handling:
  on_error: retry
  retry:
    max: 3
    backoff: linear
retry_profiles:
  unused:
    max: 1
    retriable_status_codes: []
steps:
  - id: call
    type: cli
    on_error: retry
    args: ["echo", "x"]
""")
    engine = PipelineEngine()
    engine.register_runner("cli", _FailOnceRunner())
    result = await engine.run(pipeline)

    assert result.success is True
    assert call_count == 2
