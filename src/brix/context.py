"""Pipeline execution context — holds state, outputs, credentials."""
import json
import os
import shutil
import uuid
from pathlib import Path
from typing import Any

from brix.models import Pipeline

WORKDIR_BASE = Path("/tmp/brix")


class PipelineContext:
    """Holds pipeline execution state."""

    def __init__(
        self,
        pipeline_input: dict = None,
        credentials: dict = None,
        workdir: Path = None,
        resume_from: str = None,
    ):
        self.run_id = f"run-{uuid.uuid4().hex[:12]}"
        self.input = pipeline_input or {}
        self.credentials = credentials or {}
        self.step_outputs: dict[str, Any] = {}  # step_id → output
        self.workdir = workdir or (WORKDIR_BASE / self.run_id)
        self.workdir.mkdir(parents=True, exist_ok=True)
        (self.workdir / "step_outputs").mkdir(exist_ok=True)
        (self.workdir / "files").mkdir(exist_ok=True)
        self._resume_from = resume_from

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
        """Store step output in memory AND persist to workdir."""
        self.step_outputs[step_id] = output
        # Persist for resume
        output_file = self.workdir / "step_outputs" / f"{step_id}.json"
        try:
            output_file.write_text(json.dumps(output, default=str))
        except (TypeError, ValueError):
            pass  # Non-serializable output — skip persistence

    def get_output(self, step_id: str) -> Any:
        """Get a step's output."""
        return self.step_outputs.get(step_id)

    def save_run_metadata(self, pipeline_name: str, status: str = "running") -> None:
        """Save run metadata for resume."""
        meta = {
            "run_id": self.run_id,
            "pipeline": pipeline_name,
            "input": self.input,
            "status": status,
            "completed_steps": list(self.step_outputs.keys()),
        }
        meta_file = self.workdir / "run.json"
        meta_file.write_text(json.dumps(meta, default=str, indent=2))

    @classmethod
    def from_resume(cls, run_id: str) -> "PipelineContext":
        """Resume a previous run by loading workdir state."""
        workdir = WORKDIR_BASE / run_id
        if not workdir.exists():
            raise FileNotFoundError(f"Workdir not found: {workdir}")

        meta_file = workdir / "run.json"
        if not meta_file.exists():
            raise FileNotFoundError(f"No run.json in {workdir}")

        meta = json.loads(meta_file.read_text())

        ctx = cls(
            pipeline_input=meta.get("input", {}),
            workdir=workdir,
            resume_from=run_id,
        )
        ctx.run_id = run_id

        # Reload step outputs from persisted files
        outputs_dir = workdir / "step_outputs"
        for output_file in outputs_dir.glob("*.json"):
            step_id = output_file.stem
            try:
                ctx.step_outputs[step_id] = json.loads(output_file.read_text())
            except (json.JSONDecodeError, ValueError):
                pass

        return ctx

    def is_step_completed(self, step_id: str) -> bool:
        """Check if a step was already completed (for resume)."""
        if self._resume_from:
            return step_id in self.step_outputs
        return False

    def save_file(self, filename: str, content: bytes) -> Path:
        """Save a file to the workdir and return the path."""
        file_path = self.workdir / "files" / filename
        file_path.write_bytes(content)
        return file_path

    def cleanup(self, keep: bool = False) -> None:
        """Remove workdir. Skip if keep=True."""
        if keep or not self.workdir.exists():
            return
        shutil.rmtree(self.workdir, ignore_errors=True)

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
