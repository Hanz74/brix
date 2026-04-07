"""MCP handler for pipeline smoke checks."""
from __future__ import annotations


async def _handle_smoke_test(params: dict) -> dict:
    from brix.connections import ConnectionManager
    from brix.db import BrixDB
    from brix.helper_registry import HelperRegistry
    from brix.pipeline_store import PipelineStore
    from brix.validator import PipelineValidator

    project = params.get("project")
    limit = int(params.get("limit", 5))

    db = BrixDB()
    ps = PipelineStore()
    validator = PipelineValidator()
    helper_reg = HelperRegistry(db=db)

    pipelines = db.list_pipelines()
    if project:
        pipelines = [p for p in pipelines if p.get("project") == project]
    pipelines = pipelines[:limit]

    results: list[dict] = []
    passed = 0
    failed = 0

    try:
        conn_mgr = ConnectionManager(db)
        known_conns = {c["name"] for c in conn_mgr.list()}
    except Exception:
        known_conns = set()

    for p_info in pipelines:
        name = p_info["name"]
        issues: list[str] = []

        try:
            pipeline = ps.load(name)
            load_ok = True
        except Exception as exc:
            load_ok = False
            issues.append(f"Load failed: {exc}")
            results.append(
                {
                    "pipeline": name,
                    "project": p_info.get("project", ""),
                    "load": "fail",
                    "preflight": "skip",
                    "helpers": "skip",
                    "connections": "skip",
                    "sub_pipelines": "skip",
                    "issues": issues,
                }
            )
            failed += 1
            continue

        try:
            val_result = validator.validate(pipeline, level="quick")
            preflight_ok = val_result.is_valid
            if not preflight_ok:
                issues.extend(val_result.errors[:3])
        except Exception as exc:
            preflight_ok = False
            issues.append(f"Preflight error: {exc}")

        helpers_ok = True
        for step in pipeline.steps:
            config = getattr(step, "config", None) or {}
            helper_name = getattr(step, "helper", None) or config.get("helper")
            if helper_name:
                entry = helper_reg.get(helper_name)
                if entry is None:
                    helpers_ok = False
                    issues.append(f"Step {step.id}: helper {helper_name} not found")
                elif not getattr(entry, "code", ""):
                    helpers_ok = False
                    issues.append(f"Step {step.id}: helper {helper_name} has no code")

        connections_ok = True
        for step in pipeline.steps:
            config = getattr(step, "config", None) or {}
            conn = getattr(step, "connection", None) or config.get("connection")
            if conn and conn not in known_conns:
                connections_ok = False
                issues.append(f"Step {step.id}: connection {conn} not found")

        sub_ok = True
        for step in pipeline.steps:
            config = getattr(step, "config", None) or {}
            sub = getattr(step, "pipeline", None) or config.get("pipeline")
            if sub:
                try:
                    ps.load(sub)
                except Exception:
                    sub_ok = False
                    issues.append(f"Step {step.id}: sub-pipeline {sub} not found")

        all_ok = load_ok and preflight_ok and helpers_ok and connections_ok and sub_ok
        if all_ok:
            passed += 1
        else:
            failed += 1

        results.append(
            {
                "pipeline": name,
                "project": p_info.get("project", ""),
                "load": "pass" if load_ok else "fail",
                "preflight": "pass" if preflight_ok else "fail",
                "helpers": "pass" if helpers_ok else "fail",
                "connections": "pass" if connections_ok else "fail",
                "sub_pipelines": "pass" if sub_ok else "fail",
                "issues": issues,
            }
        )

    return {
        "success": failed == 0,
        "tested": len(results),
        "passed": passed,
        "failed": failed,
        "results": results,
    }
