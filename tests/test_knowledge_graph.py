import json

from brix.db import BrixDB
from brix.knowledge_graph import build_graph_projection, query_component_relationships


def test_build_graph_projection_includes_knowledge_and_derived_edges(tmp_path):
    db = BrixDB(db_path=tmp_path / "graph.db")
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-graph-intent",
        "Map HMK relationships",
        project="buddy",
    )
    decision = db.knowledge_entity_add(
        "decision",
        "hmk-graph-decision",
        "Replace helpers with reusable bricks",
        project="buddy",
    )
    pipeline_id = db.upsert_pipeline(
        name="buddy-hmk-graph",
        path="/tmp/buddy-hmk-graph.yaml",
        project="buddy",
        tags=["graph"],
    )
    helper_id = db.upsert_helper(
        name="att_onedrive_save",
        script_path="/tmp/att_onedrive_save.py",
        description="Legacy helper",
        project="buddy",
        tags=["legacy"],
        imports=["shared_utils"],
    )
    imported_helper_id = db.upsert_helper(
        name="shared_utils",
        script_path="/tmp/shared_utils.py",
        description="Shared HMK utilities",
        project="buddy",
        tags=["utility"],
    )
    with db._connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_helper (pipeline_id, helper_id) VALUES (?, ?)",
            (pipeline_id, helper_id),
        )
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-hmk-graph-001",
                "buddy-hmk-graph",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps({"extract": {"status": "error", "error_message": "boom"}}),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )
    db.knowledge_link_add("intent", intent["id"], "created_for", "pipeline", pipeline_id)
    db.knowledge_link_add("decision", decision["id"], "replaces", "helper", helper_id)

    graph = build_graph_projection(db=db, project="buddy")
    node_keys = {(node["entity_type"], node["entity_id"]) for node in graph["nodes"]}
    edge_keys = {
        (edge["source"], edge["relation_type"], edge["target"])
        for edge in graph["edges"]
    }

    assert ("intent", intent["id"]) in node_keys
    assert ("pipeline", pipeline_id) in node_keys
    assert ("helper", helper_id) in node_keys
    assert ("helper", imported_helper_id) in node_keys
    assert ("run", "run-hmk-graph-001") in node_keys
    assert ("finding", "run-hmk-graph-001:extract") in node_keys

    assert ("intent:" + intent["id"], "created_for", "pipeline:" + pipeline_id) in edge_keys
    assert ("pipeline:" + pipeline_id, "uses", "helper:" + helper_id) in edge_keys
    assert ("helper:" + helper_id, "depends_on", "helper:" + imported_helper_id) in edge_keys
    assert ("run:run-hmk-graph-001", "executed", "pipeline:" + pipeline_id) in edge_keys
    assert ("run:run-hmk-graph-001", "has_finding", "finding:run-hmk-graph-001:extract") in edge_keys


def test_query_component_relationships_traverses_projected_graph(tmp_path):
    db = BrixDB(db_path=tmp_path / "graph-traverse.db")
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-traverse-intent",
        "Trace HMK graph",
        project="buddy",
    )
    pipeline_id = db.upsert_pipeline(
        name="buddy-hmk-traverse",
        path="/tmp/buddy-hmk-traverse.yaml",
        project="buddy",
        tags=["graph"],
    )
    helper_id = db.upsert_helper(
        name="hmk_helper",
        script_path="/tmp/hmk_helper.py",
        description="HMK helper",
        project="buddy",
        tags=["graph"],
    )
    with db._connect() as conn:
        conn.execute(
            "INSERT OR IGNORE INTO pipeline_helper (pipeline_id, helper_id) VALUES (?, ?)",
            (pipeline_id, helper_id),
        )
    db.knowledge_link_add("intent", intent["id"], "created_for", "pipeline", pipeline_id)

    subgraph = query_component_relationships(
        "pipeline",
        "buddy-hmk-traverse",
        db=db,
        depth=2,
        project="buddy",
    )

    neighbor_keys = {
        (node["entity_type"], node["entity_id"])
        for node in subgraph["neighbors"]
    }
    assert ("intent", intent["id"]) in neighbor_keys
    assert ("helper", helper_id) in neighbor_keys


def test_build_graph_projection_respects_project_scope_for_imports(tmp_path):
    db = BrixDB(db_path=tmp_path / "graph-scope.db")
    buddy_helper_id = db.upsert_helper(
        name="buddy_helper",
        script_path="/tmp/buddy_helper.py",
        description="Buddy helper",
        project="buddy",
        tags=["graph"],
        imports=["utility_helper"],
    )
    utility_helper_id = db.upsert_helper(
        name="utility_helper",
        script_path="/tmp/utility_helper.py",
        description="Utility helper",
        project="utility",
        tags=["graph"],
    )

    graph = build_graph_projection(db=db, project="buddy")
    node_keys = {(node["entity_type"], node["entity_id"]) for node in graph["nodes"]}
    edge_keys = {
        (edge["source"], edge["relation_type"], edge["target"])
        for edge in graph["edges"]
    }

    assert ("helper", buddy_helper_id) in node_keys
    assert ("helper", utility_helper_id) not in node_keys
    assert (
        "helper:" + buddy_helper_id,
        "depends_on",
        "helper:" + utility_helper_id,
    ) not in edge_keys


def test_build_graph_projection_skips_broken_knowledge_links(tmp_path):
    db = BrixDB(db_path=tmp_path / "graph-broken.db")
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-broken-link-intent",
        "Broken link intent",
        project="buddy",
    )
    pipeline_id = db.upsert_pipeline(
        name="buddy-hmk-broken-link",
        path="/tmp/buddy-hmk-broken-link.yaml",
        project="buddy",
        tags=["graph"],
    )
    db.knowledge_link_add("intent", intent["id"], "created_for", "pipeline", pipeline_id)
    with db._connect() as conn:
        conn.execute("DELETE FROM pipeline WHERE id=?", (pipeline_id,))

    graph = build_graph_projection(db=db, project="buddy")
    edge_keys = {
        (edge["source"], edge["relation_type"], edge["target"])
        for edge in graph["edges"]
    }
    node_keys = {(node["entity_type"], node["entity_id"]) for node in graph["nodes"]}

    assert ("intent:" + intent["id"], "created_for", "pipeline:" + pipeline_id) not in edge_keys
    assert ("pipeline", pipeline_id) not in node_keys
