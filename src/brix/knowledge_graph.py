from __future__ import annotations

import json
from collections import deque
from typing import Any, Optional

from brix.db import BrixDB


def _node_key(entity_type: str, entity_id: str) -> str:
    return f"{entity_type}:{entity_id}"


def _node_payload(entity_type: str, entity_id: str, entity: Optional[dict]) -> dict[str, Any]:
    payload = dict(entity or {})
    payload.setdefault("entity_type", entity_type)
    payload.setdefault("entity_id", entity_id)
    payload.setdefault("label", payload.get("title") or payload.get("name") or entity_id)
    return payload


def _add_node(nodes: dict[str, dict], entity_type: str, entity_id: str, entity: Optional[dict]) -> None:
    nodes[_node_key(entity_type, entity_id)] = _node_payload(entity_type, entity_id, entity)


def _add_edge(
    edges: list[dict[str, Any]],
    seen_edges: set[tuple[str, str, str]],
    *,
    source_type: str,
    source_id: str,
    target_type: str,
    target_id: str,
    relation_type: str,
    derived: bool,
    metadata: Optional[dict] = None,
) -> None:
    key = (_node_key(source_type, source_id), relation_type, _node_key(target_type, target_id))
    if key in seen_edges:
        return
    seen_edges.add(key)
    edges.append(
        {
            "source": key[0],
            "target": key[2],
            "relation_type": relation_type,
            "derived": derived,
            "metadata": metadata or {},
        }
    )


def build_graph_projection(
    db: Optional[BrixDB] = None,
    *,
    project: Optional[str] = None,
) -> dict[str, list[dict[str, Any]]]:
    """Project persisted knowledge and structural relationships into a graph."""
    db = db or BrixDB()
    nodes: dict[str, dict[str, Any]] = {}
    edges: list[dict[str, Any]] = []
    seen_edges: set[tuple[str, str, str]] = set()

    # First-class knowledge links.
    for link in db.knowledge_link_list():
        source = db._resolve_knowledge_link_entity(link["source_entity_type"], link["source_entity_id"])
        target = db._resolve_knowledge_link_entity(link["target_entity_type"], link["target_entity_id"])
        if source is None or target is None:
            continue
        if project:
            projects = {
                str((source or {}).get("project") or ""),
                str((target or {}).get("project") or ""),
            } - {""}
            if projects and project not in projects:
                continue
        _add_node(nodes, link["source_entity_type"], link["source_entity_id"], source)
        _add_node(nodes, link["target_entity_type"], link["target_entity_id"], target)
        _add_edge(
            edges,
            seen_edges,
            source_type=link["source_entity_type"],
            source_id=link["source_entity_id"],
            target_type=link["target_entity_type"],
            target_id=link["target_entity_id"],
            relation_type=link["relation_type"],
            derived=False,
            metadata=link.get("metadata"),
        )

    # Pipeline -> helper usage.
    for pipeline in db.list_pipelines(project=project):
        pipeline_id = pipeline["id"]
        _add_node(nodes, "pipeline", pipeline_id, pipeline)
        for helper in db.get_pipeline_helpers(pipeline["name"]):
            helper_id = helper["id"]
            _add_node(nodes, "helper", helper_id, helper)
            _add_edge(
                edges,
                seen_edges,
                source_type="pipeline",
                source_id=pipeline_id,
                target_type="helper",
                target_id=helper_id,
                relation_type="uses",
                derived=True,
            )

    # Helper import dependencies.
    helpers = {helper["name"]: helper for helper in db.list_helpers(project=project)}
    for helper in helpers.values():
        helper_id = helper["id"]
        _add_node(nodes, "helper", helper_id, helper)
        for imported_name in helper.get("imports", []):
            imported = helpers.get(imported_name) or db.get_helper(imported_name)
            if imported is None:
                continue
            if project and imported.get("project") not in ("", project):
                continue
            imported_id = imported["id"]
            _add_node(nodes, "helper", imported_id, imported)
            _add_edge(
                edges,
                seen_edges,
                source_type="helper",
                source_id=helper_id,
                target_type="helper",
                target_id=imported_id,
                relation_type="depends_on",
                derived=True,
            )

    # Run -> pipeline and run -> finding.
    with db._connect() as conn:
        if project:
            rows = conn.execute(
                "SELECT run_id, pipeline, project, steps_data, success FROM run WHERE project=? ORDER BY started_at DESC",
                (project,),
            ).fetchall()
        else:
            rows = conn.execute(
                "SELECT run_id, pipeline, project, steps_data, success FROM run ORDER BY started_at DESC"
            ).fetchall()

    for run_id, pipeline_name, run_project, steps_data, success in rows:
        run_entity = {
            "run_id": run_id,
            "pipeline": pipeline_name,
            "project": run_project or "",
            "success": success,
        }
        _add_node(nodes, "run", run_id, run_entity)
        pipeline = db.get_pipeline(pipeline_name)
        if pipeline is not None:
            pipeline_id = pipeline["id"]
            _add_node(nodes, "pipeline", pipeline_id, pipeline)
            _add_edge(
                edges,
                seen_edges,
                source_type="run",
                source_id=run_id,
                target_type="pipeline",
                target_id=pipeline_id,
                relation_type="executed",
                derived=True,
            )
        try:
            steps = json.loads(steps_data or "{}")
        except (json.JSONDecodeError, TypeError):
            steps = {}
        for step_id, step_data in steps.items():
            finding_id = f"{run_id}:{step_id}"
            finding = {
                "id": finding_id,
                "run_id": run_id,
                "step_id": step_id,
                "project": run_project or "",
                "data": step_data,
            }
            _add_node(nodes, "finding", finding_id, finding)
            _add_edge(
                edges,
                seen_edges,
                source_type="run",
                source_id=run_id,
                target_type="finding",
                target_id=finding_id,
                relation_type="has_finding",
                derived=True,
            )

    return {
        "nodes": list(nodes.values()),
        "edges": edges,
    }


def query_component_relationships(
    entity_type: str,
    entity_id: str,
    *,
    db: Optional[BrixDB] = None,
    depth: int = 1,
    relation_types: Optional[list[str]] = None,
    project: Optional[str] = None,
) -> dict[str, Any]:
    """Traverse the projected graph from one entity and return the visited subgraph."""
    db = db or BrixDB()
    graph = build_graph_projection(db=db, project=project)
    node_index = {
        _node_key(node["entity_type"], node["entity_id"]): node
        for node in graph["nodes"]
    }
    start_id = db._knowledge_link_entity_canonical_id(entity_type, entity_id) or entity_id
    start_key = _node_key(entity_type, start_id)
    if start_key not in node_index:
        raise ValueError(f"Entity '{entity_id}' of type '{entity_type}' not found in graph")

    adjacency: dict[str, list[dict[str, Any]]] = {}
    for edge in graph["edges"]:
        if relation_types and edge["relation_type"] not in relation_types:
            continue
        adjacency.setdefault(edge["source"], []).append(edge)
        adjacency.setdefault(edge["target"], []).append(edge)

    visited = {start_key}
    queue: deque[tuple[str, int]] = deque([(start_key, 0)])
    sub_edges: list[dict[str, Any]] = []
    seen_sub_edges: set[tuple[str, str, str]] = set()

    while queue:
        current, current_depth = queue.popleft()
        if current_depth >= depth:
            continue
        for edge in adjacency.get(current, []):
            edge_key = (edge["source"], edge["relation_type"], edge["target"])
            if edge_key not in seen_sub_edges:
                seen_sub_edges.add(edge_key)
                sub_edges.append(edge)
            neighbor = edge["target"] if edge["source"] == current else edge["source"]
            if neighbor in visited:
                continue
            visited.add(neighbor)
            queue.append((neighbor, current_depth + 1))

    sub_nodes = [node for node in graph["nodes"] if _node_key(node["entity_type"], node["entity_id"]) in visited]
    neighbors = [
        node
        for node in sub_nodes
        if _node_key(node["entity_type"], node["entity_id"]) != start_key
    ]
    return {
        "start": node_index[start_key],
        "nodes": sub_nodes,
        "edges": sub_edges,
        "neighbors": neighbors,
    }
