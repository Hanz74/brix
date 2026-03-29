"""Pydantic models for pipelines, steps, results, and configuration."""

from __future__ import annotations

from typing import Any, Literal, Optional, Union

from pydantic import BaseModel, Field, field_validator, model_validator


class ExtractionRule(BaseModel):
    """One extraction rule within a specialist step."""

    name: str  # Field name in output dict
    method: str  # "regex", "json_path", "split", "template"
    pattern: Optional[str] = None  # Regex pattern or JSON-path (dot-notation)
    template: Optional[str] = None  # Jinja2 template string (method="template")
    default: Any = None  # Returned when nothing matches
    group: int = 0  # Regex capture-group index (0 = full match)
    findall: bool = False  # Use re.findall instead of re.search


class ValidationRule(BaseModel):
    """One validation rule applied to an extracted field."""

    field: str  # Which extracted field to validate
    rule: str  # "required", "min_length", "max_length", "regex", "type"
    value: Any = None  # Threshold / pattern / type-name for the rule
    on_fail: str = "warn"  # "warn", "skip", "error"


class SpecialistConfig(BaseModel):
    """Deklarative Extraktion: input → extract → validate → output."""

    input_field: str = "text"  # Welches Feld aus dem Context gelesen wird

    # Extract Phase
    extract: list[ExtractionRule]  # Was extrahiert werden soll

    # Validate Phase (optional)
    # Renamed to 'checks' to avoid shadowing Pydantic's BaseModel.validate() method.
    checks: Optional[list[ValidationRule]] = None

    # Output
    output_format: str = "dict"  # "dict", "list", "flat"


class TemplateParam(BaseModel):
    """Parameter declaration for a pipeline blueprint template (T-BRIX-V8-08).

    Template parameters are placeholders used in ``{{ tpl.X }}`` expressions
    throughout a template pipeline's steps.  When ``instantiate_template`` is
    called the caller supplies concrete values that replace those expressions in
    all step fields.
    """

    name: str  # Parameter name (referenced as {{ tpl.<name> }})
    description: str  # Human-readable explanation of what the parameter does
    type: str = "string"  # string | integer | boolean | enum
    required: bool = True
    default: Any = None  # Default value when required=False
    enum_values: Optional[list[str]] = None  # Allowed values when type=enum


class InputParam(BaseModel):
    """Parameter definition for pipeline inputs."""

    type: str
    default: Any = None
    description: Optional[str] = None


class CredentialRef(BaseModel):
    """Reference to a credential stored in an environment variable."""

    env: str
    refresh: Optional[dict] = None


class RetryConfig(BaseModel):
    """Retry configuration for error handling."""

    max: int = 3
    backoff: Literal["linear", "exponential"] = "exponential"


class RetryProfile(BaseModel):
    """Named retry profile with configurable retriable HTTP status codes.

    Profiles are defined at the pipeline level under ``retry_profiles`` and
    referenced by name on individual steps via the ``retry_profile`` field.
    When a step resolves a profile, the runner uses ``retriable_status_codes``
    to decide whether a non-2xx response should trigger a retry attempt.
    """

    max: int = 3
    backoff: Literal["linear", "exponential"] = "exponential"
    # HTTP/MCP status codes that are considered transient and should be retried.
    # Empty list means all failures are retried (same behaviour as RetryConfig).
    retriable_status_codes: list[int] = Field(default_factory=list)


class ErrorConfig(BaseModel):
    """Error handling configuration for pipelines and steps."""

    on_error: Literal["stop", "continue", "retry"] = "stop"
    retry: Optional[RetryConfig] = None


class Step(BaseModel):
    """A single pipeline step."""

    id: str
    type: Literal[
        # Legacy flat names (backward-compatible, resolved via LEGACY_ALIASES in engine)
        "python", "http", "cli", "mcp", "pipeline", "pipeline_group",
        "filter", "transform", "set", "stop", "choose", "parallel", "repeat",
        "notify", "approval", "validate", "specialist",
        # New runners registered via discover_runners()
        "db_query", "db_upsert", "llm_batch", "markitdown", "source",
        "switch", "merge", "error_handler", "wait", "dedup", "aggregate",
        "flatten", "diff", "respond",
        # Advanced flow runners (T-BRIX-DB-22)
        "queue", "emit",
        # Brick-First dot-notation names (T-BRIX-DB-05c)
        "script.python", "http.request", "mcp.call", "script.cli",
        "flow.filter", "flow.transform", "flow.set", "flow.repeat",
        "flow.choose", "flow.parallel", "flow.pipeline", "flow.pipeline_group",
        "flow.validate", "flow.switch", "flow.merge", "flow.error_handler",
        "flow.wait", "flow.dedup", "flow.aggregate", "flow.flatten", "flow.diff",
        "action.notify", "action.approval", "action.respond",
        "extract.specialist",
        "db.query", "db.upsert",
        "llm.batch",
        "markitdown.convert",
        "source.fetch",
    ]

    # Step enablement — disabled steps are unconditionally skipped
    enabled: bool = True

    # Python runner
    script: Optional[str] = None
    # Helper registry reference — resolved to script path at run time (T-BRIX-V4-BUG-12)
    helper: Optional[str] = None

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

    # pipeline_group runner (T-BRIX-V6-17): run multiple sub-pipelines in parallel
    pipelines: Optional[list[str]] = None
    shared_params: dict[str, Any] = Field(default_factory=dict)

    # set runner
    values: Optional[dict[str, Any]] = None
    persist: bool = False  # T-BRIX-DB-13: when True, write values to persistent_store

    # stop runner
    message: Optional[str] = None
    success_on_stop: bool = True

    # notify runner (T-BRIX-V4-11)
    channel: Optional[str] = None
    to: Optional[str] = None

    # approval runner (T-BRIX-V4-12)
    approval_timeout: str = "24h"  # Timeout duration, e.g. "30m", "1h", "24h"
    on_timeout: str = "stop"       # "stop" or "continue"

    # choose runner (T-BRIX-V4-05)
    choices: Optional[list[dict]] = None       # [{when: str, steps: list[dict]}, ...]
    default_steps: Optional[list[dict]] = None  # Default branch when no choice matches

    # parallel step runner (T-BRIX-V4-06)
    sub_steps: Optional[list[dict]] = None     # Steps to run in parallel

    # repeat runner (T-BRIX-V4-07)
    until: Optional[str] = None                # Jinja2 condition — stop when true
    while_condition: Optional[str] = None      # Jinja2 condition — continue while true
    max_iterations: int = 100                  # Safety limit
    sequence: Optional[list[dict]] = None      # Steps to repeat

    # Common to all runners
    params: Optional[dict[str, Any]] = None

    # Iteration / parallelism
    foreach: Optional[str] = None
    parallel: bool = False
    concurrency: int = 10
    batch_size: int = 0  # 0 = no batching, >0 = process in chunks of this size

    # Iteration output mode
    flat_output: bool = False  # If true, foreach returns [data, data, ...] instead of {items, summary}

    # Conditional execution
    when: Optional[str] = None
    else_of: Optional[str] = None  # "This step runs when step X was skipped"

    # Per-step error override
    on_error: Optional[Literal["stop", "continue", "retry"]] = None

    # Named retry profile reference — must match a key in Pipeline.retry_profiles
    retry_profile: Optional[str] = None

    # Timeout string, e.g. "30s", "5m"
    timeout: Optional[str] = None

    # Auto-pagination: follow @odata.nextLink / Link header until exhausted
    fetch_all_pages: bool = False

    # Intra-step progress: parse BRIX_PROGRESS lines from helper stderr (T-BRIX-V4-BUG-05)
    progress: bool = False

    # Per-step Python package requirements — checked and auto-installed before step execution (T-BRIX-V6-03)
    requirements: list[str] = Field(default_factory=list)

    # Schema-Contracts (T-BRIX-V6-13): optional field-level schema declarations
    # Simple dict of {field_name: type_hint} — used for inter-step compatibility checks
    input_schema: dict = Field(default_factory=dict)
    output_schema: dict = Field(default_factory=dict)

    # validate runner fields (T-BRIX-V6-15)
    rules: Optional[list[dict]] = None  # [{field, min_ratio, of, on_fail}, ...]

    # specialist runner fields (T-BRIX-V8-03)
    # Inline specialist config — ExtractionRules + ValidationRules + output_format.
    # Use this instead of a Python helper for declarative data extraction.
    config: Optional[dict] = None  # Parsed into SpecialistConfig at run time

    # DAG execution (T-BRIX-V6-19): explicit step dependencies
    # When any step declares depends_on, the engine switches to DAG mode:
    # steps without dependencies run in parallel; steps with dependencies
    # wait until all named steps have completed successfully.
    depends_on: list[str] = Field(default_factory=list)

    # Step-Level Caching (T-BRIX-V6-24): when True (legacy bool), the step output
    # is stored in a content-addressed cache keyed by (step_id + resolved params).
    # When a dict (T-BRIX-DB-21 resilience cache), supports key/ttl:
    #   {"key": "{{ jinja }}", "ttl": "1h"}
    # Both forms cache on success and serve the cached result on hit.
    cache: Union[bool, dict, None] = False

    # Resilience: Circuit Breaker (T-BRIX-DB-21)
    # {"max_failures": 3, "cooldown": "10m", "fallback": "step_id"}
    circuit_breaker: Optional[dict] = None

    # Resilience: Rate Limiter (T-BRIX-DB-21)
    # {"max_calls": 100, "per": "1m"}
    rate_limit: Optional[dict] = None

    # Saga Compensation (T-BRIX-DB-21)
    # Step dict that is executed if a later step in the pipeline fails.
    # {"type": "...", "config": {...}} or any valid step-like dict.
    compensate: Optional[dict] = None

    # Execution Data (T-BRIX-V7-04): when True, persist full step output,
    # rendered params, context snapshot, and stderr to the step_outputs table.
    # Also triggered automatically when BRIX_DEBUG=true env var is set.
    persist_output: bool = False

    # Breakpoints (T-BRIX-V7-06): when True, the engine pauses before executing
    # this step by writing a breakpoint.json sentinel to the run workdir.
    # The run resumes when brix__resume_run deletes the sentinel.
    pause_before: bool = False

    # Run-Persistenz (T-BRIX-DB-07): when False, input_data and output_data are NOT
    # stored in step_executions (only status/timing).  Use for steps that process
    # sensitive data that must not be written to the database.
    persist_data: bool = True

    # Profile / Mixin (T-BRIX-DB-23): named profile loaded from DB at runtime.
    # Profile config fields act as defaults; step-level fields override them.
    profile: Optional[str] = None

    # Queue/Buffer runner fields (T-BRIX-DB-22)
    queue_name: Optional[str] = None      # Unique queue identifier
    collect_until: Optional[int] = None   # Flush after N items
    collect_for: Optional[str] = None     # Flush after time window ("5m", "1h")
    flush_to: Optional[str] = None        # Informational: target step_id after flush

    # Emit runner fields (T-BRIX-DB-22)
    event: Optional[str] = None           # Event name to emit/listen to
    data: Any = None                      # Payload for emit (Jinja2 rendered)

    # Streaming — EXPERIMENTAL (T-BRIX-DB-22)
    # When True on a foreach step, the engine MAY begin processing items as soon
    # as the first item is available rather than waiting for the full list.
    # Full implementation requires asyncio generator support — marked experimental.
    stream: bool = False

    # Per-step unwrap_json override (T-BRIX-IMP-01).
    # When set to True or False, overrides the server-level unwrap_json config.
    # When None (default), falls back to the ServerConfig.unwrap_json setting.
    unwrap_json: Optional[bool] = None

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


class MattermostNotifyConfig(BaseModel):
    """Mattermost webhook notification configuration (T-BRIX-V6-06)."""

    enabled: bool = False
    webhook_url: str = ""


class PipelineNotifyConfig(BaseModel):
    """Notification hooks that fire after a pipeline completes (T-BRIX-V6-06)."""

    mattermost: MattermostNotifyConfig = Field(default_factory=MattermostNotifyConfig)


class Pipeline(BaseModel):
    """A Brix pipeline definition."""

    name: str
    version: str = "0.1.0"
    description: Optional[str] = None
    brix_version: Optional[str] = None

    # Pipeline-Template inheritance (T-BRIX-V6-18)
    # kind: "template" marks this pipeline as a base template (not directly runnable).
    kind: Optional[str] = None
    # extends: name of the base template pipeline this pipeline inherits from.
    extends: Optional[str] = None
    # template_params: values substituted into {{ template.X }} placeholders in base template.
    template_params: dict = Field(default_factory=dict)

    # Pipeline Blueprints (T-BRIX-V8-08)
    # is_template: marks this pipeline as an instantiatable blueprint.
    is_template: bool = False
    # blueprint_params: declared parameters for {{ tpl.X }} placeholders in step fields.
    blueprint_params: list[TemplateParam] = Field(default_factory=list)

    input: dict[str, InputParam] = Field(default_factory=dict)
    credentials: dict[str, CredentialRef] = Field(default_factory=dict)
    error_handling: ErrorConfig = Field(default_factory=ErrorConfig)
    # Named retry profiles available to steps in this pipeline
    retry_profiles: dict[str, RetryProfile] = Field(default_factory=dict)
    # Post-run notification hooks (T-BRIX-V6-06)
    notify: PipelineNotifyConfig = Field(default_factory=PipelineNotifyConfig)

    # Pipeline-Idempotency (T-BRIX-V6-22): Jinja2 expression evaluated at run
    # start. If a finished run with the same resolved key exists in the last 24h
    # the engine returns that run's result instead of starting a new run.
    idempotency_key: Optional[str] = None

    # Compositor-Mode (T-BRIX-V8-07): when True, LLM must use built-in bricks
    # and MCP steps only. python / cli steps are blocked at execution time
    # unless allow_code is explicitly set to True.
    compositor_mode: bool = False
    # allow_code: override to permit python/cli steps even in compositor_mode.
    # Default True for backward-compatibility. When compositor_mode=True this
    # is set to False automatically unless the caller explicitly passes True.
    allow_code: bool = True
    # strict_bricks (T-BRIX-DB-05d): when True, using legacy step-type names
    # raises an error instead of emitting a deprecation warning.
    # Compositor-Mode sets this automatically.
    strict_bricks: bool = False

    # Test-Mode (T-BRIX-DB-24): when True, db.upsert steps are dry (logged but
    # not written to DB) and action.notify steps are log-only (no real sends).
    # Used in combination with step pins for pipeline unit testing.
    test_mode: bool = False

    @model_validator(mode="after")
    def _apply_compositor_defaults(self) -> "Pipeline":
        """If compositor_mode is True and allow_code was not explicitly set
        to True by the caller, flip allow_code to False."""
        if self.compositor_mode and self.allow_code is True:
            # Only flip when the field still holds its default value.
            # We detect "explicitly set" via model_fields_set.
            if "allow_code" not in self.model_fields_set:
                object.__setattr__(self, "allow_code", False)
        return self

    @field_validator("credentials", mode="before")
    @classmethod
    def coerce_credentials(cls, v: Any) -> Any:
        """Accept shorthand credential syntax.

        In addition to the full form ``{env: "MY_ENV_VAR"}``, also accept
        a plain string value: ``credentials: {MY_KEY: "cred-uuid-..."}``
        is coerced to ``{MY_KEY: {env: "cred-uuid-..."}}``.
        """
        if not isinstance(v, dict):
            return v
        result: dict[str, Any] = {}
        for key, val in v.items():
            if isinstance(val, str):
                result[key] = {"env": val}
            else:
                result[key] = val
        return result
    # Python package requirements — installed automatically before step execution (T-BRIX-V4-BUG-11)
    requirements: list[str] = Field(default_factory=list)
    # Named step groups for reuse via include: (T-BRIX-V4-17)
    groups: dict[str, list[dict]] = Field(default_factory=dict)
    steps: list[Step] = Field(min_length=1)
    output: Optional[dict[str, str]] = None

    # Pipeline-Composition (T-BRIX-V6-14): named output slots exposed to callers
    # Maps slot_name → Jinja2-expression evaluated after sub-pipeline completes
    output_slots: dict[str, str] = Field(default_factory=dict)

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


class StepProgress(BaseModel):
    """Intra-step progress info from BRIX_PROGRESS stderr lines (T-BRIX-V4-BUG-05)."""

    processed: int = 0
    total: int = 0
    percent: float = 0.0
    eta_seconds: Optional[float] = None
    message: Optional[str] = None


class StepStatus(BaseModel):
    """Status entry for a step in a run result."""

    status: Literal["ok", "error", "skipped", "dry_run"]
    duration: float
    items: Optional[int] = None
    errors: Optional[int] = None
    reason: Optional[str] = None  # for skipped steps
    error_message: Optional[str] = None  # human-readable error detail for failed steps
    step_progress: Optional["StepProgress"] = None  # intra-step progress (T-BRIX-V4-BUG-05)
    resource_usage: Optional[dict] = None  # {rss_mb, duration} — T-BRIX-V7-07


class RunResult(BaseModel):
    """Overall result of a pipeline run."""

    success: bool
    run_id: str
    steps: dict[str, StepStatus]
    result: Any
    duration: float
    # Deprecation warnings accumulated during the run (T-BRIX-DB-05d)
    deprecation_warnings: list[str] = Field(default_factory=list)


class ServerConfig(BaseModel):
    """Configuration for an MCP server.

    Supports two transports:
    - ``stdio`` (default): launch a subprocess and communicate over stdin/stdout.
      Requires ``command`` to be set.
    - ``sse``: connect to an already-running HTTP/SSE MCP server.
      Requires ``url`` to be set; ``command`` is ignored.
    """

    name: str
    command: str = ""
    args: list[str] = Field(default_factory=list)
    env: dict[str, str] = Field(default_factory=dict)
    tools_prefix: Optional[str] = None
    transport: str = "stdio"  # "stdio" or "sse"
    url: str = ""  # SSE endpoint URL (used when transport="sse")
    unwrap_json: bool = False  # Auto-unwrap nested JSON strings in responses (e.g. Cody)
