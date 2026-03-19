"""Pydantic models for pipelines, steps, results, and configuration."""

from __future__ import annotations

from typing import Any, Literal, Optional

from pydantic import BaseModel, Field, field_validator, model_validator


class InputParam(BaseModel):
    """Parameter definition for pipeline inputs."""

    type: str
    default: Any = None
    description: Optional[str] = None


class CredentialRef(BaseModel):
    """Reference to a credential stored in an environment variable."""

    env: str


class RetryConfig(BaseModel):
    """Retry configuration for error handling."""

    max: int = 3
    backoff: Literal["linear", "exponential"] = "exponential"


class ErrorConfig(BaseModel):
    """Error handling configuration for pipelines and steps."""

    on_error: Literal["stop", "continue", "retry"] = "stop"
    retry: Optional[RetryConfig] = None


class Step(BaseModel):
    """A single pipeline step."""

    id: str
    type: Literal["python", "http", "cli", "mcp", "pipeline"]

    # Python runner
    script: Optional[str] = None

    # HTTP runner
    url: Optional[str] = None
    method: str = "GET"
    headers: Optional[dict[str, str]] = None
    body: Any = None

    # CLI runner
    command: Optional[str] = None
    args: Optional[list[str]] = None
    shell: bool = False

    # MCP runner
    server: Optional[str] = None
    tool: Optional[str] = None

    # Pipeline (sub-pipeline) runner
    pipeline: Optional[str] = None

    # Common to all runners
    params: Optional[dict[str, Any]] = None

    # Iteration / parallelism
    foreach: Optional[str] = None
    parallel: bool = False
    concurrency: int = 10

    # Conditional execution
    when: Optional[str] = None

    # Per-step error override
    on_error: Optional[Literal["stop", "continue", "retry"]] = None

    # Timeout string, e.g. "30s", "5m"
    timeout: Optional[str] = None

    @field_validator("concurrency")
    @classmethod
    def concurrency_must_be_positive(cls, v: int) -> int:
        if v <= 0:
            raise ValueError("concurrency must be greater than 0")
        return v

    @model_validator(mode="after")
    def validate_type_constraints(self) -> "Step":
        if self.type == "mcp":
            if not self.server or not self.tool:
                raise ValueError(
                    "Steps of type 'mcp' must have both 'server' and 'tool' set"
                )
        if self.type == "cli":
            if self.shell:
                if not self.command:
                    raise ValueError(
                        "Steps of type 'cli' with shell=True must have 'command' set"
                    )
            else:
                # shell=False: args required, or command will be split into args
                if not self.args and not self.command:
                    raise ValueError(
                        "Steps of type 'cli' with shell=False must have 'args' or 'command' set"
                    )
        return self


class Pipeline(BaseModel):
    """A Brix pipeline definition."""

    name: str
    version: str = "0.1.0"
    description: Optional[str] = None
    brix_version: Optional[str] = None

    input: dict[str, InputParam] = Field(default_factory=dict)
    credentials: dict[str, CredentialRef] = Field(default_factory=dict)
    error_handling: ErrorConfig = Field(default_factory=ErrorConfig)
    steps: list[Step] = Field(min_length=1)
    output: Optional[dict[str, str]] = None

    @field_validator("steps")
    @classmethod
    def steps_not_empty(cls, v: list[Step]) -> list[Step]:
        if len(v) < 1:
            raise ValueError("Pipeline must have at least one step")
        return v


class StepResult(BaseModel):
    """Result of a single step execution."""

    success: bool
    data: Any = None
    duration: float = 0.0
    items_count: Optional[int] = None
    error: Optional[str] = None


class ForeachItem(BaseModel):
    """A single item result within a foreach execution."""

    success: bool
    data: Any = None
    error: Optional[str] = None
    input: Any = None


class ForeachSummary(BaseModel):
    """Summary counts for a foreach execution."""

    total: int
    succeeded: int
    failed: int


class ForeachResult(BaseModel):
    """Result of a foreach step execution (D-15)."""

    items: list[ForeachItem]
    summary: ForeachSummary


class StepStatus(BaseModel):
    """Status entry for a step in a run result."""

    status: Literal["ok", "error", "skipped"]
    duration: float
    items: Optional[int] = None
    errors: Optional[int] = None
    reason: Optional[str] = None  # for skipped steps


class RunResult(BaseModel):
    """Overall result of a pipeline run."""

    success: bool
    run_id: str
    steps: dict[str, StepStatus]
    result: Any
    duration: float


class ServerConfig(BaseModel):
    """Configuration for an MCP server."""

    name: str
    command: str
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    tools_prefix: Optional[str] = None
