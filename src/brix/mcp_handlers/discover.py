"""Universal Registry — discover(), list/get runners, env_config, types, namespaces.

Implements T-BRIX-DB-19: Universal Registry with lückenlose list/get/search.

Handlers:
- brix__discover          — overview of all categories or detail/search
- brix__list_runners      — all registered runners with schema
- brix__get_runner_info   — detail for a single runner
- brix__list_env_config   — all BRIX_* ENV-Vars with current value and default
- brix__list_types        — type compatibility matrix
- brix__list_namespaces   — brick namespaces with their bricks
"""
from __future__ import annotations

import logging
import os

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Category metadata — what each category contains and which tool to use
# ---------------------------------------------------------------------------

_CATEGORY_META: dict[str, dict] = {
    "bricks": {
        "description": "Pipeline building blocks (built-in + MCP-discovered)",
        "tool": "brix__list_bricks",
        "search_tool": "brix__search_bricks",
        "get_tool": "brix__get_brick_schema",
    },
    "pipelines": {
        "description": "Saved pipeline definitions",
        "tool": "brix__list_pipelines",
        "search_tool": "brix__search_pipelines",
        "get_tool": "brix__get_pipeline",
    },
    "runners": {
        "description": "Step execution backends (python, http, mcp, cli, …)",
        "tool": "brix__list_runners",
        "search_tool": None,
        "get_tool": "brix__get_runner_info",
    },
    "connectors": {
        "description": "Source connector abstractions (M365, OneDrive, …)",
        "tool": "brix__list_connectors",
        "search_tool": None,
        "get_tool": "brix__get_connector",
    },
    "connections": {
        "description": "Named DB connections",
        "tool": "brix__connection_list",
        "search_tool": None,
        "get_tool": None,
    },
    "credentials": {
        "description": "Stored secrets and API keys",
        "tool": "brix__credential_list",
        "search_tool": "brix__credential_search",
        "get_tool": "brix__credential_get",
    },
    "variables": {
        "description": "Managed pipeline variables",
        "tool": "brix__list_variables",
        "search_tool": None,
        "get_tool": "brix__get_variable",
    },
    "templates": {
        "description": "Parameterised pipeline blueprints",
        "tool": "brix__list_templates",
        "search_tool": None,
        "get_tool": None,
    },
    "triggers": {
        "description": "Scheduled and event-based pipeline triggers",
        "tool": "brix__trigger_list",
        "search_tool": None,
        "get_tool": "brix__trigger_get",
    },
    "alerts": {
        "description": "Pipeline alerting rules",
        "tool": "brix__alert_list",
        "search_tool": None,
        "get_tool": None,
    },
    "types": {
        "description": "Type compatibility matrix for pipeline step I/O",
        "tool": "brix__list_types",
        "search_tool": None,
        "get_tool": None,
    },
    "jinja_filters": {
        "description": "Available Jinja2 filters in pipeline templates",
        "tool": "brix__discover",
        "search_tool": None,
        "get_tool": None,
    },
    "env_config": {
        "description": "BRIX_* environment variables with defaults",
        "tool": "brix__list_env_config",
        "search_tool": None,
        "get_tool": None,
    },
    "namespaces": {
        "description": "Brick namespaces (flow, db, source, action, …)",
        "tool": "brix__list_namespaces",
        "search_tool": None,
        "get_tool": None,
    },
    "helpers": {
        "description": "Registered Python helper scripts",
        "tool": "brix__list_helpers",
        "search_tool": "brix__search_helpers",
        "get_tool": "brix__get_helper",
    },
    "runs": {
        "description": "Pipeline run history",
        "tool": "brix__get_run_history",
        "search_tool": "brix__run_search",
        "get_tool": "brix__get_run_status",
    },
}


# ---------------------------------------------------------------------------
# Internal helpers
# ---------------------------------------------------------------------------

def _get_registry():
    """Import shared BrickRegistry singleton lazily to avoid circular imports."""
    from brix.mcp_handlers._shared import _registry
    return _registry


def _get_pipeline_store():
    from brix.mcp_handlers._shared import _store
    return _store


def _get_runner_info(name: str, cls) -> dict:
    """Build a runner info dict from a runner class."""
    try:
        instance = cls()
        config_schema = instance.config_schema()
        input_type = instance.input_type()
        output_type = instance.output_type()
    except Exception as e:
        config_schema = {}
        input_type = "unknown"
        output_type = "unknown"
        logger.debug("Could not instantiate runner '%s': %s", name, e)

    return {
        "name": name,
        "class": cls.__name__,
        "module": cls.__module__,
        "description": (cls.__doc__ or "").strip().split("\n")[0].strip(),
        "input_type": input_type,
        "output_type": output_type,
        "config_schema": config_schema,
    }


def _count_category(category: str) -> int:
    """Return the item count for a category (best-effort, 0 on error)."""
    try:
        if category == "bricks":
            return _get_registry().count
        elif category == "pipelines":
            return len(_get_pipeline_store().list_all())
        elif category == "runners":
            from brix.runners.base import discover_runners
            return len(discover_runners())
        elif category == "connectors":
            from brix.connectors import list_connectors
            return len(list_connectors())
        elif category == "connections":
            from brix.db import BrixDB
            db = BrixDB()
            rows = db.connection_list()
            return len(rows)
        elif category == "credentials":
            from brix.credential_store import CredentialStore
            cs = CredentialStore()
            return len(cs.list_all())
        elif category == "variables":
            from brix.db import BrixDB
            db = BrixDB()
            rows = db.variable_list()
            return len(rows)
        elif category == "templates":
            store = _get_pipeline_store()
            all_info = store.list_all()
            count = 0
            for info in all_info:
                try:
                    raw = store.load_raw(info["name"])
                    if raw.get("is_template", False):
                        count += 1
                except Exception:
                    pass
            return count
        elif category == "triggers":
            from brix.db import BrixDB
            db = BrixDB()
            rows = db.trigger_list()
            return len(rows)
        elif category == "alerts":
            from brix.db import BrixDB
            db = BrixDB()
            rows = db.alert_list()
            return len(rows)
        elif category == "types":
            from brix.bricks.types import TYPE_COMPATIBILITY
            return len(TYPE_COMPATIBILITY)
        elif category == "jinja_filters":
            from jinja2.sandbox import SandboxedEnvironment
            env = SandboxedEnvironment()
            return len(env.filters)
        elif category == "env_config":
            from brix.config import BrixConfig
            import inspect
            attrs = [
                k for k, v in vars(BrixConfig()).items()
                if not k.startswith("_") and not callable(v)
            ]
            return len(attrs)
        elif category == "namespaces":
            reg = _get_registry()
            ns = set()
            for b in reg.list_all():
                ns.add(b.namespace or "")
            return len(ns)
        elif category == "helpers":
            from brix.helper_registry import HelperRegistry
            hr = HelperRegistry()
            return len(hr.list_all())
        elif category == "runs":
            from brix.db import BrixDB
            db = BrixDB()
            rows = db.run_list(limit=1000)
            return len(rows)
    except Exception as e:
        logger.debug("Could not count category '%s': %s", category, e)
    return 0


def _search_in_category(category: str, query: str) -> list[dict]:
    """Search items in a category by name and description matching query."""
    query_lower = query.lower()
    results = []

    try:
        if category == "bricks":
            for b in _get_registry().list_all():
                text = f"{b.name} {b.description} {b.when_to_use}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": b.name,
                        "description": b.description,
                        "match_field": "name/description/when_to_use",
                    })

        elif category == "pipelines":
            for p in _get_pipeline_store().list_all():
                text = f"{p.get('name','')} {p.get('description','')}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": p.get("name", ""),
                        "description": p.get("description", ""),
                        "match_field": "name/description",
                    })

        elif category == "runners":
            from brix.runners.base import discover_runners
            for name, cls in discover_runners().items():
                desc = (cls.__doc__ or "").strip().split("\n")[0].strip()
                text = f"{name} {desc}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": name,
                        "description": desc,
                        "match_field": "name/description",
                    })

        elif category == "connectors":
            from brix.connectors import list_connectors
            for c in list_connectors():
                text = f"{c.name} {c.description}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": c.name,
                        "description": c.description,
                        "match_field": "name/description",
                    })

        elif category == "helpers":
            from brix.helper_registry import HelperRegistry
            hr = HelperRegistry()
            for h in hr.list_all():
                name = h.get("name", "")
                desc = h.get("description", "")
                text = f"{name} {desc}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": name,
                        "description": desc,
                        "match_field": "name/description",
                    })

        elif category == "types":
            from brix.bricks.types import TYPE_COMPATIBILITY
            for type_name, compatible_with in TYPE_COMPATIBILITY.items():
                text = f"{type_name} {' '.join(compatible_with)}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": type_name,
                        "description": f"Compatible with: {compatible_with}",
                        "match_field": "type_name/compatible_types",
                    })

        elif category == "jinja_filters":
            from jinja2.sandbox import SandboxedEnvironment
            env = SandboxedEnvironment()
            for filter_name in env.filters:
                if query_lower in filter_name.lower():
                    results.append({
                        "category": category,
                        "name": filter_name,
                        "description": f"Jinja2 filter: {filter_name}",
                        "match_field": "name",
                    })

        elif category == "namespaces":
            reg = _get_registry()
            ns_map: dict[str, list[str]] = {}
            for b in reg.list_all():
                ns = b.namespace or ""
                ns_map.setdefault(ns, []).append(b.name)
            for ns_name, bricks in ns_map.items():
                text = f"{ns_name} {' '.join(bricks)}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": ns_name or "(root)",
                        "description": f"{len(bricks)} bricks: {', '.join(bricks[:5])}{'...' if len(bricks) > 5 else ''}",
                        "match_field": "namespace/bricks",
                    })

        elif category == "env_config":
            from brix.config import BrixConfig
            cfg = BrixConfig()
            for attr, value in vars(cfg).items():
                if attr.startswith("_") or callable(value):
                    continue
                env_key = f"BRIX_{attr}"
                text = f"{env_key} {attr}".lower()
                if query_lower in text:
                    results.append({
                        "category": category,
                        "name": env_key,
                        "description": f"Current value: {value}",
                        "match_field": "env_var/attr",
                    })

    except Exception as e:
        logger.debug("Search in category '%s' failed: %s", category, e)

    return results


# ---------------------------------------------------------------------------
# Handler: brix__discover
# ---------------------------------------------------------------------------

async def _handle_discover(arguments: dict) -> dict:
    """Universal discovery across all Brix registry categories.

    Without parameters: returns all categories with counts and tool names.
    With category: returns details of that category.
    With query: searches across ALL categories by name + description.
    """
    category = (arguments.get("category") or "").strip()
    query = (arguments.get("query") or "").strip()

    # --- Query mode: search across all categories ---
    if query and not category:
        all_results = []
        for cat in _CATEGORY_META:
            cat_results = _search_in_category(cat, query)
            all_results.extend(cat_results)

        by_category: dict[str, list[dict]] = {}
        for item in all_results:
            cat = item["category"]
            by_category.setdefault(cat, []).append(item)

        return {
            "success": True,
            "query": query,
            "total_matches": len(all_results),
            "results": all_results,
            "by_category": {cat: items for cat, items in by_category.items()},
            "categories_with_matches": list(by_category.keys()),
        }

    # --- Category detail mode ---
    if category:
        if category not in _CATEGORY_META:
            available = sorted(_CATEGORY_META.keys())
            return {
                "success": False,
                "error": f"Unknown category '{category}'. Available: {available}",
                "available_categories": available,
            }

        meta = _CATEGORY_META[category]
        items: list[dict] = []

        try:
            if category == "bricks":
                for b in _get_registry().list_all():
                    items.append({
                        "name": b.name,
                        "type": b.type,
                        "namespace": b.namespace,
                        "category": b.category,
                        "description": b.description,
                        "input_type": b.input_type,
                        "output_type": b.output_type,
                    })

            elif category == "pipelines":
                for p in _get_pipeline_store().list_all():
                    items.append({
                        "name": p.get("name", ""),
                        "description": p.get("description", ""),
                        "version": p.get("version", ""),
                        "step_count": p.get("step_count", 0),
                    })

            elif category == "runners":
                from brix.runners.base import discover_runners
                for name, cls in sorted(discover_runners().items()):
                    items.append(_get_runner_info(name, cls))

            elif category == "connectors":
                from brix.connectors import list_connectors
                for c in list_connectors():
                    items.append({
                        "name": c.name,
                        "type": c.type,
                        "description": c.description,
                        "required_mcp_server": c.required_mcp_server,
                        "parameter_count": len(c.parameters),
                    })

            elif category == "connections":
                from brix.db import BrixDB
                db = BrixDB()
                rows = db.connection_list()
                for r in rows:
                    items.append({
                        "name": r.get("name", ""),
                        "type": r.get("type", ""),
                        "description": r.get("description", ""),
                    })

            elif category == "credentials":
                from brix.credential_store import CredentialStore
                cs = CredentialStore()
                for c in cs.list_all():
                    items.append({
                        "name": c.get("name", ""),
                        "type": c.get("type", ""),
                        "description": c.get("description", ""),
                        "has_value": bool(c.get("value") or c.get("encrypted_value")),
                    })

            elif category == "variables":
                from brix.db import BrixDB
                db = BrixDB()
                rows = db.variable_list()
                for r in rows:
                    items.append({
                        "name": r.get("name", ""),
                        "value": r.get("value"),
                        "description": r.get("description", ""),
                        "scope": r.get("scope", "global"),
                    })

            elif category == "templates":
                store = _get_pipeline_store()
                for info in store.list_all():
                    try:
                        raw = store.load_raw(info["name"])
                        if raw.get("is_template", False):
                            items.append({
                                "name": info["name"],
                                "description": raw.get("description", ""),
                                "blueprint_params": [
                                    p.get("name") for p in raw.get("blueprint_params", [])
                                ],
                                "step_count": len(raw.get("steps", [])),
                            })
                    except Exception:
                        pass

            elif category == "triggers":
                from brix.db import BrixDB
                db = BrixDB()
                rows = db.trigger_list()
                for r in rows:
                    items.append({
                        "name": r.get("name", ""),
                        "type": r.get("type", ""),
                        "pipeline": r.get("pipeline", ""),
                        "enabled": r.get("enabled", False),
                    })

            elif category == "alerts":
                from brix.db import BrixDB
                db = BrixDB()
                rows = db.alert_list()
                for r in rows:
                    items.append({
                        "name": r.get("name", ""),
                        "pipeline": r.get("pipeline", ""),
                        "event": r.get("event", ""),
                        "channel": r.get("channel", ""),
                    })

            elif category == "types":
                from brix.bricks.types import _get_type_compatibility
                compat = _get_type_compatibility()
                for type_name, compatible_with in sorted(compat.items()):
                    items.append({
                        "name": type_name,
                        "compatible_with": compatible_with,
                        "compatible_count": len(compatible_with),
                    })

            elif category == "jinja_filters":
                from jinja2.sandbox import SandboxedEnvironment
                env = SandboxedEnvironment()
                for filter_name in sorted(env.filters.keys()):
                    items.append({
                        "name": filter_name,
                        "description": f"Jinja2 built-in filter",
                    })

            elif category == "env_config":
                from brix.config import BrixConfig
                cfg = BrixConfig()
                default_cfg = BrixConfig()
                for attr, value in sorted(vars(cfg).items()):
                    if attr.startswith("_") or callable(value):
                        continue
                    env_key = f"BRIX_{attr}"
                    current = os.environ.get(env_key)
                    items.append({
                        "name": env_key,
                        "attr": attr,
                        "current_value": value,
                        "env_override": current,
                        "is_overridden": current is not None,
                    })

            elif category == "namespaces":
                reg = _get_registry()
                ns_map: dict[str, list[str]] = {}
                for b in reg.list_all():
                    ns = b.namespace or ""
                    ns_map.setdefault(ns, []).append(b.name)
                for ns_name, bricks in sorted(ns_map.items()):
                    items.append({
                        "namespace": ns_name or "(root)",
                        "brick_count": len(bricks),
                        "bricks": bricks,
                    })

            elif category == "helpers":
                from brix.helper_registry import HelperRegistry
                hr = HelperRegistry()
                for h in hr.list_all():
                    items.append({
                        "name": h.get("name", ""),
                        "description": h.get("description", ""),
                        "path": h.get("path", ""),
                        "type": h.get("type", ""),
                    })

            elif category == "runs":
                from brix.db import BrixDB
                db = BrixDB()
                rows = db.run_list(limit=50)
                for r in rows:
                    items.append({
                        "run_id": r.get("run_id", ""),
                        "pipeline": r.get("pipeline", ""),
                        "status": r.get("status", ""),
                        "started_at": r.get("started_at", ""),
                    })

        except Exception as e:
            logger.warning("Error listing category '%s': %s", category, e)

        return {
            "success": True,
            "category": category,
            "description": meta["description"],
            "tool": meta["tool"],
            "search_tool": meta.get("search_tool"),
            "get_tool": meta.get("get_tool"),
            "count": len(items),
            "items": items,
        }

    # --- Overview mode: all categories with counts ---
    overview = []
    for cat, meta in _CATEGORY_META.items():
        count = _count_category(cat)
        overview.append({
            "category": cat,
            "description": meta["description"],
            "count": count,
            "tool": meta["tool"],
            "search_tool": meta.get("search_tool"),
            "get_tool": meta.get("get_tool"),
        })

    total = sum(item["count"] for item in overview)
    return {
        "success": True,
        "total_items": total,
        "category_count": len(overview),
        "categories": overview,
        "usage": {
            "overview": "brix__discover() — all categories with counts",
            "category_detail": "brix__discover(category='runners') — items in one category",
            "global_search": "brix__discover(query='email') — search across all categories",
        },
    }


# ---------------------------------------------------------------------------
# Handler: brix__list_runners
# ---------------------------------------------------------------------------

async def _handle_list_runners(arguments: dict) -> dict:
    """List all available pipeline step runners.

    Returns a list of runners with name, config_schema, input_type,
    output_type, and description. Use brix__get_runner_info for full details.
    """
    from brix.runners.base import discover_runners

    try:
        runners = discover_runners()
    except Exception as e:
        return {"success": False, "error": f"Could not discover runners: {e}"}

    items = []
    for name in sorted(runners.keys()):
        cls = runners[name]
        items.append(_get_runner_info(name, cls))

    return {
        "success": True,
        "count": len(items),
        "runners": items,
    }


# ---------------------------------------------------------------------------
# Handler: brix__get_runner_info
# ---------------------------------------------------------------------------

async def _handle_get_runner_info(arguments: dict) -> dict:
    """Get full details for a single runner by name.

    Returns name, class, module, description, input_type, output_type,
    and the full config_schema for that runner.
    """
    name = (arguments.get("name") or "").strip()
    if not name:
        return {"success": False, "error": "Parameter 'name' is required"}

    from brix.runners.base import discover_runners

    try:
        runners = discover_runners()
    except Exception as e:
        return {"success": False, "error": f"Could not discover runners: {e}"}

    if name not in runners:
        available = sorted(runners.keys())
        return {
            "success": False,
            "error": f"Runner '{name}' not found.",
            "available_runners": available,
        }

    cls = runners[name]
    info = _get_runner_info(name, cls)

    # Also include full docstring
    info["full_description"] = (cls.__doc__ or "").strip()

    return {"success": True, "runner": info}


# ---------------------------------------------------------------------------
# Handler: brix__list_env_config
# ---------------------------------------------------------------------------

async def _handle_list_env_config(arguments: dict) -> dict:
    """List all BRIX_* environment variables with current value and default.

    Returns all configurable Brix settings read from environment.
    Shows whether each var is currently overridden via ENV.
    """
    from brix.config import BrixConfig

    try:
        cfg = BrixConfig()
    except Exception as e:
        return {"success": False, "error": f"Could not load BrixConfig: {e}"}

    items = []
    for attr in sorted(vars(cfg).keys()):
        if attr.startswith("_"):
            continue
        value = getattr(cfg, attr)
        if callable(value):
            continue
        env_key = f"BRIX_{attr}"
        env_override = os.environ.get(env_key)
        items.append({
            "env_var": env_key,
            "attr": attr,
            "current_value": value,
            "type": type(value).__name__,
            "env_override": env_override,
            "is_overridden": env_override is not None,
        })

    return {
        "success": True,
        "count": len(items),
        "env_config": items,
        "note": "Set BRIX_<ATTR> environment variable to override any value.",
    }


# ---------------------------------------------------------------------------
# Handler: brix__list_types
# ---------------------------------------------------------------------------

async def _handle_list_types(arguments: dict) -> dict:
    """List all pipeline step I/O types with their compatibility matrix.

    Returns all known types, what each type is compatible with, and
    converter brick suggestions where available.
    """
    from brix.bricks.types import _get_type_compatibility, _CONVERTER_SUGGESTIONS

    try:
        compat = _get_type_compatibility()
    except Exception as e:
        return {"success": False, "error": f"Could not load type compatibility: {e}"}

    items = []
    for type_name in sorted(compat.keys()):
        compatible_with = compat[type_name]

        # Find converter suggestions for this type
        converters = []
        for (out_prefix, inp_prefix, brick) in _CONVERTER_SUGGESTIONS:
            if type_name.startswith(out_prefix):
                converters.append({
                    "target_type_prefix": inp_prefix,
                    "converter_brick": brick,
                })

        items.append({
            "name": type_name,
            "compatible_with": compatible_with,
            "compatible_count": len(compatible_with),
            "is_wildcard": type_name in ("*", "any"),
            "is_none": type_name == "none",
            "converter_suggestions": converters,
        })

    return {
        "success": True,
        "count": len(items),
        "types": items,
        "note": (
            "Use brix.bricks.types.is_compatible(output_type, input_type) "
            "to check step I/O compatibility. '*' and '' match any type."
        ),
    }


# ---------------------------------------------------------------------------
# Handler: brix__list_namespaces
# ---------------------------------------------------------------------------

async def _handle_list_namespaces(arguments: dict) -> dict:
    """List all brick namespaces with their member bricks.

    Returns namespaces like flow, db, source, action, extract, script,
    http, mcp, llm, markitdown with the bricks they contain.
    """
    reg = _get_registry()
    all_bricks = reg.list_all()

    ns_map: dict[str, list[dict]] = {}
    for b in all_bricks:
        ns = b.namespace or ""
        ns_map.setdefault(ns, [])
        ns_map[ns].append({
            "name": b.name,
            "type": b.type,
            "category": b.category,
            "description": b.description,
            "input_type": b.input_type,
            "output_type": b.output_type,
        })

    items = []
    for ns_name in sorted(ns_map.keys()):
        bricks = ns_map[ns_name]
        items.append({
            "namespace": ns_name or "(root)",
            "brick_count": len(bricks),
            "bricks": bricks,
        })

    return {
        "success": True,
        "namespace_count": len(items),
        "namespaces": items,
        "note": (
            "Namespaces group bricks by domain. "
            "Use brix__get_brick_schema(brick_name) for full parameter details."
        ),
    }
