"""Pipeline execution context — holds state, outputs, credentials."""
import os
import uuid
from typing import Any

from brix.models import Pipeline


class PipelineContext:
    """Holds pipeline execution state."""

    def __init__(self, pipeline_input: dict = None, credentials: dict = None):
        self.run_id = f"run-{uuid.uuid4().hex[:12]}"
        self.input = pipeline_input or {}
        self.credentials = credentials or {}
        self.step_outputs: dict[str, Any] = {}  # step_id → output

    @classmethod
    def from_pipeline(cls, pipeline: Pipeline, user_input: dict = None) -> "PipelineContext":
        """Create context from a Pipeline model.

        Resolves credentials from environment variables.
        Merges user_input with pipeline defaults.
        """
        # Merge input: user_input overrides pipeline defaults
        resolved_input: dict[str, Any] = {}
        for key, param in pipeline.input.items():
            if user_input and key in user_input:
                resolved_input[key] = user_input[key]
            elif param.default is not None:
                resolved_input[key] = param.default

        # Resolve credentials from ENV
        resolved_credentials: dict[str, Any] = {}
        for key, cred in pipeline.credentials.items():
            value = os.environ.get(cred.env, "")
            resolved_credentials[key] = value

        ctx = cls(pipeline_input=resolved_input, credentials=resolved_credentials)
        return ctx

    def set_output(self, step_id: str, output: Any) -> None:
        """Store a step's output."""
        self.step_outputs[step_id] = output

    def get_output(self, step_id: str) -> Any:
        """Get a step's output."""
        return self.step_outputs.get(step_id)

    def to_jinja_context(self, item: Any = None) -> dict:
        """Build Jinja2 template context.

        Context contains:
        - input.*  — pipeline input parameters
        - credentials.* — resolved credential values
        - <step_id>.output — outputs from previous steps (wrapped in namespace)
        - item — current foreach item (if any)
        """
        ctx: dict[str, Any] = {
            "input": self.input,
            "credentials": self.credentials,
        }
        # Add step outputs as step_id with .output accessor
        for step_id, output in self.step_outputs.items():
            ctx[step_id] = {"output": output}

        if item is not None:
            ctx["item"] = item

        return ctx
