"""Shared engine-facing types and config helpers."""

from dataclasses import dataclass
from typing import Any

from brix.loader import PipelineLoader
from brix.models import Step, StepStatus

_SPECIALIST_STEP_TYPES = {"specialist", "extract.specialist"}

# Top-level Step attributes that should be visible to runner.validate_config().
# Derive this from the Step model so new dedicated step fields do not need a
# second manual allowlist in the engine.
_VALIDATE_CONFIG_TOP_LEVEL_FIELDS: tuple[str, ...] = tuple(
    field_name
    for field_name in Step.model_fields.keys()
    if field_name not in {"id", "type", "config"}
)


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


class _RenderedStep:
    """Wraps a Step with rendered Jinja2 values for the runner."""

    def __init__(self, step: Step, rendered: dict, loader: PipelineLoader, jinja_ctx: dict):
        # Copy original step attributes
        self.id = step.id
        self.type = step.type
        self.timeout = step.timeout
        self.shell = step.shell

        # Use rendered values where available, fall back to originals
        self.args = rendered.get("_args") or (
            [loader.render_value(a, jinja_ctx) for a in step.args] if step.args else None
        )
        self.command = rendered.get("_command") or (
            loader.render_value(step.command, jinja_ctx) if step.command else None
        )
        self.url = rendered.get("_url") or step.url
        self.headers = rendered.get("_headers") or step.headers
        self.body = rendered["_body"] if "_body" in rendered else step.body
        self.method = step.method
        self.script = step.script
        self.server = step.server
        self.tool = step.tool
        self.config = rendered.get("_config") if "_config" in rendered else getattr(step, "config", None)
        self.pipeline = rendered.get("_pipeline") or step.pipeline
        self.params = rendered if rendered else _step_config_dict(step)
        # set runner: rendered values under _values key, fall back to raw values field
        self.values = rendered.get("_values") or getattr(step, "values", None) or {}
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
