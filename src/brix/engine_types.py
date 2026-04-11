"""Shared engine-facing types and config helpers."""

from dataclasses import dataclass
import json
import logging
import os
import sys
from typing import Any

from brix.loader import PipelineLoader
from brix.models import Step, StepStatus
from brix.materialize import materialize_step
from brix.serialization import json_dumps, sanitize_for_json

_SPECIALIST_STEP_TYPES = {"specialist", "extract.specialist"}

# Top-level Step attributes that should be visible to runner.validate_config().
# Derive this from the Step model so new dedicated step fields do not need a
# second manual allowlist in the engine.
_VALIDATE_CONFIG_TOP_LEVEL_FIELDS: tuple[str, ...] = tuple(
    field_name
    for field_name in Step.model_fields.keys()
    if field_name not in {"id", "type", "config"}
)

_log_level_name = os.environ.get("BRIX_LOG_LEVEL", "INFO").upper()
_log_level = getattr(logging, _log_level_name, logging.INFO)


def _extract_brick_default_values(raw_schema: Any) -> dict[str, Any]:
    """Return runtime default values from a brick ``config_schema`` payload.

    Supported shapes:
    - Legacy/custom flat defaults: ``{"server": "cody", "tool": "x"}``
    - BrickParam/JSON-schema-like dicts: ``{"timeout": {"type": "string", "default": "60s"}}``

    Keys without an explicit ``default`` are ignored for schema-shaped entries so
    metadata like ``{"type": "string"}`` is never mistaken for a live param value.
    """
    if not isinstance(raw_schema, dict) or not raw_schema:
        return {}

    defaults: dict[str, Any] = {}
    for key, value in raw_schema.items():
        if isinstance(value, dict):
            if "default" in value:
                defaults[key] = value.get("default")
            continue
        default_attr = getattr(value, "default", None)
        if default_attr is not None:
            defaults[key] = default_attr
            continue
        defaults[key] = value
    return defaults


class _JsonFormatter(logging.Formatter):
    """Emit one JSON object per log record to stderr."""

    def format(self, record: logging.LogRecord) -> str:
        return json.dumps(
            {
                "timestamp": self.formatTime(record, "%Y-%m-%dT%H:%M:%S"),
                "level": record.levelname,
                "component": record.name,
                "message": record.getMessage(),
            }
        )


def _build_logger(name: str) -> logging.Logger:
    logger = logging.getLogger(name)
    if not logger.handlers:
        handler = logging.StreamHandler(sys.stderr)
        handler.setFormatter(_JsonFormatter())
        logger.addHandler(handler)
        logger.propagate = False
    logger.setLevel(_log_level)
    return logger


def _db_log(level: str, component: str, message: str) -> None:
    """Write one entry to the brix.db app_log table (best-effort, never raises)."""
    try:
        from brix.db import BrixDB

        BrixDB().write_app_log(level=level, component=component, message=message)
    except Exception:
        pass


def _measure_rss_mb() -> float:
    """Return the current RSS memory usage of this process in megabytes.

    Reads /proc/self/status (Linux). Falls back to 0.0 if unavailable.
    """
    try:
        with open("/proc/self/status") as fh:
            for line in fh:
                if line.startswith("VmRSS:"):
                    kb = int(line.split()[1])
                    return round(kb / 1024.0, 2)
    except Exception:
        pass
    try:
        import resource

        kb = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        if os.uname().sysname == "Darwin":
            return round(kb / (1024.0 * 1024.0), 2)
        return round(kb / 1024.0, 2)
    except Exception:
        return 0.0


def _total_ram_mb() -> float:
    """Return total system RAM in MB from /proc/meminfo, or 0.0 if unavailable."""
    try:
        with open("/proc/meminfo") as fh:
            for line in fh:
                if line.startswith("MemTotal:"):
                    kb = int(line.split()[1])
                    return kb / 1024.0
    except Exception:
        pass
    return 0.0


def _warn_if_high_memory(rss_mb: float, step_id: str) -> None:
    """Emit a warning if RSS > 80% of total available RAM (best-effort)."""
    total_mb = _total_ram_mb()
    if total_mb <= 0.0 or rss_mb <= 0.0:
        return
    ratio = rss_mb / total_mb
    if ratio > 0.80:
        pct = round(ratio * 100, 1)
        msg = (
            f"[Resource Warning] Step '{step_id}' RSS={rss_mb:.1f}MB is {pct}% "
            f"of total RAM ({total_mb:.0f}MB). Consider reducing concurrency or batch_size."
        )
        print(msg, file=sys.stderr)
        _build_logger("brix.engine").warning(msg)


def _capture_environment() -> dict[str, Any]:
    """Capture a lightweight environment snapshot at run start."""
    import sys as _sys

    snapshot: dict[str, Any] = {
        "python_version": f"{_sys.version_info.major}.{_sys.version_info.minor}.{_sys.version_info.micro}",
    }

    try:
        dists: list[str] = []
        try:
            import importlib.metadata as _imeta

            for dist in sorted(
                _imeta.distributions(),
                key=lambda d: (d.metadata.get("Name") or "").lower(),
            ):
                name = dist.metadata.get("Name") or dist.name or ""
                version = dist.metadata.get("Version") or ""
                if name:
                    dists.append(f"{name}=={version}")
        except Exception:
            pass
        try:
            from brix import __version__ as _brix_version

            dists = [entry for entry in dists if not entry.lower().startswith("brix==")]
            dists.insert(0, f"brix=={_brix_version}")
            snapshot["brix_version"] = _brix_version
        except Exception:
            pass
        snapshot["installed_packages"] = dists[:200]
    except Exception:
        snapshot["installed_packages"] = []

    try:
        from brix.server_manager import ServerManager

        snapshot["mcp_servers"] = sorted(
            entry["name"] for entry in ServerManager().list_all()
        )
    except Exception:
        snapshot["mcp_servers"] = []

    return snapshot


def _redact_secret_values(data: Any, secret_values: set) -> Any:
    """Replace all secret variable plaintext occurrences with '***REDACTED***'."""
    if not secret_values or data is None:
        return data
    try:
        json_str = json_dumps(sanitize_for_json(data))
        for secret in secret_values:
            if secret and secret in json_str:
                json_str = json_str.replace(secret, "***REDACTED***")
        return json.loads(json_str)
    except Exception:
        return data


def _extract_step_cost(data: Any) -> float:
    """Extract LLM cost in USD from a step output dict."""
    if not isinstance(data, dict):
        return 0.0
    usage = data.get("llm_usage")
    if not isinstance(usage, dict):
        return 0.0

    input_tokens: int = int(usage.get("input_tokens") or 0)
    output_tokens: int = int(usage.get("output_tokens") or 0)
    model: str = str(usage.get("model") or "").lower()

    _PRICING: dict[str, tuple[float, float]] = {
        "mistral-large": (4.0, 12.0),
        "mistral-medium": (2.7, 8.1),
        "mistral-small": (1.0, 3.0),
        "mistral-tiny": (0.25, 0.25),
        "gpt-4o": (5.0, 15.0),
        "gpt-4o-mini": (0.15, 0.6),
        "gpt-4-turbo": (10.0, 30.0),
        "gpt-3.5-turbo": (0.5, 1.5),
        "claude-3-opus": (15.0, 75.0),
        "claude-3-sonnet": (3.0, 15.0),
        "claude-3-haiku": (0.25, 1.25),
        "claude-sonnet-4": (3.0, 15.0),
        "claude-opus-4": (15.0, 75.0),
        "gemini-1.5-pro": (3.5, 10.5),
        "gemini-1.5-flash": (0.35, 1.05),
    }

    price_in, price_out = 0.0, 0.0
    for key, (p_in, p_out) in _PRICING.items():
        if model.startswith(key) or key in model:
            price_in, price_out = p_in, p_out
            break

    cost = (input_tokens / 1_000_000) * price_in + (output_tokens / 1_000_000) * price_out
    return cost


def _step_config_dict(step: Any) -> dict[str, Any]:
    """Return the generic config dict a non-specialist runner should see."""
    params = getattr(step, "params", None)
    if isinstance(params, dict) and params:
        return dict(params)
    config_dict = getattr(step, "config", None)
    if (
        getattr(step, "type", None) not in _SPECIALIST_STEP_TYPES
        and isinstance(config_dict, dict)
        and config_dict
    ):
        return dict(config_dict)
    return {}


@dataclass
class StepResult:
    status: str
    output: Any = None
    should_abort: bool = False
    should_continue: bool = True
    cost: float = 0.0
    resource_usage: dict | None = None
    step_status: StepStatus | None = None


@dataclass
class DagSharedState:
    pipeline_aborted: bool = False
    last_output: Any = None
    stop_step_success: bool | None = None
    total_cost_usd: float = 0.0


class _RenderedStep:
    """Wraps a Step with rendered Jinja2 values for the runner."""

    def __init__(self, step: Step, rendered: Any, loader: PipelineLoader, jinja_ctx: dict):
        materialized = materialize_step(step)
        rendered_dict = rendered if isinstance(rendered, dict) else {}
        rendered_config = (
            rendered_dict.get("_config")
            if "_config" in rendered_dict
            else materialized.effective_config or getattr(step, "config", None)
        )
        if "_params" in rendered_dict:
            rendered_params = rendered_dict["_params"]
        elif isinstance(rendered, list):
            rendered_params = rendered
        elif isinstance(rendered_config, dict) and "params" in rendered_config:
            rendered_params = rendered_config.get("params")
        elif isinstance(rendered, dict):
            rendered_params = {
                key: value
                for key, value in rendered_dict.items()
                if not key.startswith("_")
            } or None
        else:
            rendered_params = rendered

        # Copy original step attributes
        self.id = step.id
        self.type = step.type
        self.effective_type = materialized.effective_type
        self.timeout = step.timeout
        self.shell = step.shell

        # Use rendered values where available, fall back to originals
        self.args = rendered_dict.get("_args") or (
            [loader.render_value(a, jinja_ctx) for a in materialized.effective_step_fields.get("args", step.args)]
            if materialized.effective_step_fields.get("args", step.args)
            else None
        )
        self.command = rendered_dict.get("_command") or (
            loader.render_value(materialized.effective_step_fields.get("command", step.command), jinja_ctx)
            if materialized.effective_step_fields.get("command", step.command)
            else None
        )
        self.url = rendered_dict.get("_url") or materialized.effective_step_fields.get("url", step.url)
        self.headers = rendered_dict.get("_headers") or materialized.effective_step_fields.get("headers", step.headers)
        self.body = rendered_dict["_body"] if "_body" in rendered_dict else materialized.effective_step_fields.get("body", step.body)
        self.method = materialized.effective_step_fields.get("method", step.method)
        self.script = materialized.effective_step_fields.get("script", step.script)
        self.server = materialized.effective_step_fields.get("server", step.server)
        self.tool = materialized.effective_step_fields.get("tool", step.tool)
        self.config = rendered_config
        self.pipeline = rendered_dict.get("_pipeline") or materialized.effective_step_fields.get("pipeline", step.pipeline)
        self.params = rendered_params if rendered_params not in (None, {}) else materialized.effective_params
        self.materialized_step = materialized
        # set runner: rendered values under _values key, fall back to raw values field
        self.values = rendered_dict.get("_values") or getattr(step, "values", None) or {}
        # set runner: persist flag (T-BRIX-DB-13)
        self.persist = getattr(step, "persist", False)
        # stop runner fields
        self.message = getattr(step, "message", None)
        self.success_on_stop = getattr(step, "success_on_stop", True)
        # choose runner fields (T-BRIX-V4-05)
        self.choices = getattr(step, "choices", None)
        self.default_steps = getattr(step, "default_steps", None)
        # parallel step runner fields (T-BRIX-V4-06)
        self.sub_steps = getattr(step, "sub_steps", None)
        # repeat runner fields (T-BRIX-V4-07)
        self.sequence = getattr(step, "sequence", None)
        self.until = getattr(step, "until", None)
        self.while_condition = getattr(step, "while_condition", None)
        self.max_iterations = getattr(step, "max_iterations", 100)
        # notify runner fields (T-BRIX-V4-11)
        self.channel = getattr(step, "channel", None)
        self.to = getattr(step, "to", None)
        # approval runner fields (T-BRIX-V4-12)
        self.approval_timeout = getattr(step, "approval_timeout", "24h")
        self.on_timeout = getattr(step, "on_timeout", "stop")
        # intra-step progress (T-BRIX-V4-BUG-05)
        self.progress = getattr(step, "progress", False)
        # pipeline_group runner fields (T-BRIX-V6-17)
        self.pipelines = getattr(step, "pipelines", None)
        self.shared_params = getattr(step, "shared_params", {}) or {}
        # concurrency is already set on Step; expose it here for pipeline_group runner
        self.concurrency = getattr(step, "concurrency", 3)

        # Backfill any Step model fields added after this wrapper was introduced.
        # Explicit assignments above win over the dynamic copy.
        for field_name in Step.model_fields:
            if field_name not in ("id", "type", "params", "config"):
                if not hasattr(self, field_name):
                    setattr(self, field_name, getattr(step, field_name, None))
