"""Base runner interface — all runners implement this."""

import importlib
import inspect
import pkgutil
import warnings
from abc import ABC, abstractmethod
from typing import Any


class BaseRunner(ABC):
    """Base interface for all pipeline step runners.

    Every concrete runner MUST implement:
    - execute()       — step execution logic
    - config_schema() — JSON schema describing accepted config keys
    - input_type()    — expected input type string
    - output_type()   — produced output type string

    Optional override:
    - validate_config() — defaults to schema-based required-field check
    """

    # Internal progress state; written by report_progress().
    _progress: dict | None = None

    @abstractmethod
    async def execute(self, step: Any, context: Any) -> dict:
        """Execute a pipeline step.

        Args:
            step: Step configuration (Step model or dict)
            context: Pipeline execution context

        Returns:
            dict with keys: success (bool), data (Any), duration (float)
            On error: success=False, error=str

        MUST call report_progress() at least once (start or end).
        """
        ...

    @abstractmethod
    def config_schema(self) -> dict:
        """Return a JSON-Schema dict describing config parameters for this runner.

        Minimal example::

            {
                "type": "object",
                "properties": {
                    "script": {"type": "string", "description": "Path to script"}
                },
                "required": ["script"]
            }
        """
        ...

    @abstractmethod
    def input_type(self) -> str:
        """Return the expected input type.

        Common values: 'none', 'list[dict]', 'dict', 'text', 'any'
        """
        ...

    @abstractmethod
    def output_type(self) -> str:
        """Return the produced output type.

        Common values: 'list[dict]', 'dict', 'text', 'any', 'none'
        """
        ...

    # ------------------------------------------------------------------
    # Concrete helpers (no override needed in most runners)
    # ------------------------------------------------------------------

    def report_progress(
        self,
        pct: float,
        msg: str = "",
        done: int = 0,
        total: int = 0,
    ) -> None:
        """Record execution progress for the engine.

        Call this at least once inside execute() — at the start or end.
        The engine may read self._progress after execute() returns.

        Args:
            pct:   Completion percentage 0.0–100.0
            msg:   Human-readable status message
            done:  Number of items processed so far
            total: Total number of items (0 = unknown)
        """
        self._progress = {"pct": pct, "msg": msg, "done": done, "total": total}

    def validate_config(self, config: dict) -> list[str]:
        """Validate *config* against this runner's config_schema().

        Default implementation checks that all ``required`` fields listed in the
        schema are present in *config*.  Returns a list of error strings;
        an empty list means the config is valid.
        """
        schema = self.config_schema()
        required: list[str] = schema.get("required", [])
        errors: list[str] = []
        for field in required:
            if field not in config:
                errors.append(f"Missing required config field: '{field}'")
        return errors

    # ------------------------------------------------------------------
    # Subclass registration hook
    # ------------------------------------------------------------------

    def __init_subclass__(cls, **kwargs: Any) -> None:
        """Auto-called when a subclass is defined.

        Verifies that any *concrete* subclass (i.e. not abstract itself)
        has not accidentally left abstract methods un-implemented.
        Python's ABC machinery raises TypeError on instantiation anyway,
        but this hook fires at class-definition time for earlier feedback.
        """
        super().__init_subclass__(**kwargs)
        # Nothing extra to do here — the ABC metaclass enforces this at
        # instantiation time via TypeError.  The hook is here as an
        # extension point for future enforcement/registration needs.


# ---------------------------------------------------------------------------
# Test/stub helper
# ---------------------------------------------------------------------------


class _StubRunnerMixin:
    """Mixin that provides no-op implementations of the new abstract methods.

    Intended for use in tests and internal stubs where the full interface
    metadata is not needed:

        class MyTestRunner(_StubRunnerMixin, BaseRunner):
            async def execute(self, step, context):
                ...
    """

    def config_schema(self) -> dict:  # type: ignore[override]
        return {"type": "object", "properties": {}}

    def input_type(self) -> str:  # type: ignore[override]
        return "any"

    def output_type(self) -> str:  # type: ignore[override]
        return "any"


# ---------------------------------------------------------------------------
# Runner auto-discovery
# ---------------------------------------------------------------------------

def discover_runners() -> dict[str, type["BaseRunner"]]:
    """Scan the brix.runners package and return a mapping of runner names.

    Each module in the package is imported; any concrete (non-abstract)
    subclass of BaseRunner found at module top-level is registered under
    the canonical step-type name derived from the class name:

        ``CliRunner``      → ``"cli"``
        ``PythonRunner``   → ``"python"``
        ``HttpRunner``     → ``"http"``
        ``McpRunner``      → ``"mcp"``
        ``FilterRunner``   → ``"filter"``
        ``TransformRunner``→ ``"transform"``
        ``SetRunner``      → ``"set"``
        ``ChooseRunner``   → ``"choose"``
        ``ParallelStepRunner`` → ``"parallel"``
        ``RepeatRunner``   → ``"repeat"``
        ``NotifyRunner``   → ``"notify"``
        ``ApprovalRunner`` → ``"approval"``
        ``ValidateRunner`` → ``"validate"``
        ``PipelineRunner`` → ``"pipeline"``
        ``PipelineGroupRunner`` → ``"pipeline_group"``
        ``SpecialistRunner``    → ``"specialist"``

    The name is derived by stripping the ``Runner`` suffix, lowercasing, and
    converting CamelCase to snake_case for multi-word names.

    Returns:
        Dict mapping step-type strings to (un-instantiated) runner classes.
    """
    import re

    _camel_re = re.compile(r"(?<=[a-z0-9])(?=[A-Z])")

    def _class_to_step_type(name: str) -> str:
        """Convert 'PipelineGroupRunner' → 'pipeline_group'."""
        # Strip trailing 'Runner'
        if name.endswith("Runner"):
            name = name[: -len("Runner")]
        # Strip trailing 'Step' (e.g. 'ParallelStep')
        if name.endswith("Step"):
            name = name[: -len("Step")]
        # CamelCase → snake_case
        return _camel_re.sub("_", name).lower()

    import brix.runners as _pkg

    registry: dict[str, type[BaseRunner]] = {}

    for module_info in pkgutil.iter_modules(_pkg.__path__):
        if module_info.name == "base":
            continue
        try:
            mod = importlib.import_module(f"brix.runners.{module_info.name}")
        except Exception as exc:  # pragma: no cover
            warnings.warn(
                f"discover_runners: could not import brix.runners.{module_info.name}: {exc}",
                stacklevel=2,
            )
            continue

        for _name, obj in inspect.getmembers(mod, inspect.isclass):
            if (
                obj is not BaseRunner
                and issubclass(obj, BaseRunner)
                and not inspect.isabstract(obj)
                # Only register classes defined in this module (avoid re-registering
                # re-exported classes that live in a different module).
                and obj.__module__ == mod.__name__
            ):
                step_type = _class_to_step_type(obj.__name__)
                registry[step_type] = obj

    return registry
