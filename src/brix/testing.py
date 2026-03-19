"""Pipeline testing with mock fixtures."""
import yaml
from pathlib import Path
from typing import Any, Optional

from brix.models import Pipeline
from brix.loader import PipelineLoader
from brix.engine import PipelineEngine
from brix.runners.base import BaseRunner


class MockRunner(BaseRunner):
    """Runner that returns pre-defined mock data instead of executing."""

    def __init__(self, mocks: dict[str, Any]):
        """mocks: dict of step_id → mock output data."""
        self._mocks = mocks

    async def execute(self, step: Any, context: Any) -> dict:
        step_id = getattr(step, "id", None)
        if step_id and step_id in self._mocks:
            return {
                "success": True,
                "data": self._mocks[step_id],
                "duration": 0.0,
            }
        # Not mocked — this shouldn't happen if fixture is correct
        return {"success": False, "error": f"Step '{step_id}' not mocked", "duration": 0.0}


class TestFixture:
    """Represents a test fixture for a pipeline."""

    def __init__(
        self,
        pipeline_path: str,
        input_data: Optional[dict] = None,
        mocks: Optional[dict] = None,
        assertions: Optional[dict] = None,
        description: str = "",
    ):
        self.pipeline_path = pipeline_path
        self.input_data = input_data or {}
        self.mocks = mocks or {}
        self.assertions = assertions or {}
        self.description = description

    @classmethod
    def load(cls, fixture_path: str) -> "TestFixture":
        """Load a test fixture from YAML."""
        with open(fixture_path) as f:
            data = yaml.safe_load(f) or {}

        return cls(
            pipeline_path=data.get("pipeline", ""),
            input_data=data.get("input", {}),
            mocks=data.get("mocks", {}),
            assertions=data.get("assertions", {}),
            description=data.get("description", ""),
        )


class AssertionResult:
    def __init__(self, step_id: str, assertion: str, passed: bool, message: str = ""):
        self.step_id = step_id
        self.assertion = assertion
        self.passed = passed
        self.message = message


class PipelineTestRunner:
    """Runs pipeline tests with mock fixtures."""

    def __init__(self):
        self.loader = PipelineLoader()

    async def run_test(self, fixture: TestFixture) -> dict:
        """Run a pipeline test with mock data.

        Returns dict with:
        - success: bool
        - run_result: RunResult
        - assertions: list of AssertionResult
        - summary: {steps_passed, steps_total, assertions_passed, assertions_total}
        """
        # Load pipeline
        pipeline = self.loader.load(fixture.pipeline_path)

        # Create engine with mock runners for mocked steps
        engine = PipelineEngine()

        # Override runners for mocked steps: wrap original runners
        # so mocked steps return mock data, others run normally
        original_runners = dict(engine._runners)
        mock_runner = _SelectiveMockRunner(fixture.mocks, original_runners)

        # Replace all runners with the selective mock
        for step_type in list(original_runners.keys()):
            engine._runners[step_type] = mock_runner

        # Run pipeline
        result = await engine.run(pipeline, fixture.input_data)

        # Check assertions
        assertion_results = []
        for step_id, step_assertions in fixture.assertions.items():
            for assertion in step_assertions:
                ar = self._check_assertion(step_id, assertion, result)
                assertion_results.append(ar)

        steps_passed = sum(1 for s in result.steps.values() if s.status in ("ok", "skipped"))
        steps_total = len(result.steps)
        assertions_passed = sum(1 for a in assertion_results if a.passed)
        assertions_total = len(assertion_results)

        return {
            "success": result.success and all(a.passed for a in assertion_results),
            "run_result": result,
            "assertions": assertion_results,
            "summary": {
                "steps_passed": steps_passed,
                "steps_total": steps_total,
                "assertions_passed": assertions_passed,
                "assertions_total": assertions_total,
            },
        }

    def _check_assertion(self, step_id: str, assertion: dict, result: Any) -> AssertionResult:
        """Check a single assertion against the run result."""
        step_status = result.steps.get(step_id)

        if "status" in assertion:
            expected = assertion["status"]
            actual = step_status.status if step_status else "missing"
            return AssertionResult(
                step_id,
                f"status == {expected}",
                actual == expected,
                f"expected {expected}, got {actual}",
            )

        if "item_count" in assertion:
            expected = assertion["item_count"]
            actual = step_status.items if step_status else 0
            return AssertionResult(
                step_id,
                f"item_count == {expected}",
                actual == expected,
                f"expected {expected}, got {actual}",
            )

        # Default: step succeeded
        return AssertionResult(
            step_id,
            "step succeeded",
            step_status is not None and step_status.status == "ok",
            f"status: {step_status.status if step_status else 'missing'}",
        )


class _SelectiveMockRunner(BaseRunner):
    """Routes to mock data or real runner based on step ID."""

    def __init__(self, mocks: dict, real_runners: dict):
        self._mocks = mocks
        self._real_runners = real_runners

    async def execute(self, step: Any, context: Any) -> dict:
        step_id = getattr(step, "id", None)
        step_type = getattr(step, "type", None)

        if step_id in self._mocks:
            return {
                "success": True,
                "data": self._mocks[step_id],
                "duration": 0.0,
            }

        # Run with real runner
        real_runner = self._real_runners.get(step_type)
        if real_runner:
            return await real_runner.execute(step, context)

        return {"success": False, "error": f"No runner for type '{step_type}'", "duration": 0.0}
