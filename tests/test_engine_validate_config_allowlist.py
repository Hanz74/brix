import ast
from pathlib import Path

from brix.engine import _VALIDATE_CONFIG_TOP_LEVEL_FIELDS
from brix.models import Step


def _runner_validation_fields() -> dict[str, set[str]]:
    """Return {RunnerClassName: top-level Step fields read during validation}."""
    runner_dir = Path(__file__).resolve().parents[1] / "src" / "brix" / "runners"
    step_fields = set(Step.model_fields.keys())
    results: dict[str, set[str]] = {}

    for path in sorted(runner_dir.glob("*.py")):
        tree = ast.parse(path.read_text(encoding="utf-8"), filename=str(path))
        for node in tree.body:
            if not isinstance(node, ast.ClassDef) or not node.name.endswith("Runner"):
                continue

            fields: set[str] = set()

            for fn in node.body:
                if not isinstance(fn, (ast.FunctionDef, ast.AsyncFunctionDef)):
                    continue

                if fn.name == "config_schema":
                    for child in ast.walk(fn):
                        if not isinstance(child, ast.Dict):
                            continue
                        keys = [
                            key.value if isinstance(key, ast.Constant) else None
                            for key in child.keys
                        ]
                        if "required" not in keys:
                            continue
                        required_node = child.values[keys.index("required")]
                        if isinstance(required_node, (ast.List, ast.Tuple)):
                            for item in required_node.elts:
                                if isinstance(item, ast.Constant) and isinstance(item.value, str):
                                    if item.value in step_fields:
                                        fields.add(item.value)

                if fn.name == "validate_config":
                    for child in ast.walk(fn):
                        if not isinstance(child, ast.Call):
                            continue
                        if not isinstance(child.func, ast.Attribute):
                            continue
                        if not isinstance(child.func.value, ast.Name):
                            continue
                        if child.func.value.id != "config" or child.func.attr != "get":
                            continue
                        if not child.args:
                            continue
                        first_arg = child.args[0]
                        if isinstance(first_arg, ast.Constant) and isinstance(first_arg.value, str):
                            if first_arg.value in step_fields:
                                fields.add(first_arg.value)

            if fields:
                results[node.name] = fields

    return results


def test_validate_config_allowlist_covers_all_step_backed_runner_fields():
    allowlist = set(_VALIDATE_CONFIG_TOP_LEVEL_FIELDS)
    missing_by_runner: dict[str, list[str]] = {}

    for runner_name, fields in _runner_validation_fields().items():
        missing = sorted(field for field in fields if field not in allowlist)
        if missing:
            missing_by_runner[runner_name] = missing

    assert missing_by_runner == {}, (
        "Engine validate_config allowlist is missing top-level Step fields used by runners: "
        f"{missing_by_runner}"
    )


def test_validate_config_allowlist_includes_known_regression_fields():
    allowlist = set(_VALIDATE_CONFIG_TOP_LEVEL_FIELDS)
    assert {
        "choices",
        "event",
        "inputs",
        "key",
        "mode",
        "on_timeout",
        "output_schema",
        "params",
        "queue_name",
        "rules",
        "sequence",
        "sub_steps",
        "try_step",
    } <= allowlist
