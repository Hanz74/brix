import json

from brix.db import BrixDB
from brix.semantic_retrieval import (
    build_token_profile,
    cosine_similarity,
    semantic_search,
    sync_semantic_index,
)


def test_token_profile_and_cosine_similarity_are_stable():
    left = build_token_profile("HMK parse failure on invoice extraction")
    right = build_token_profile("invoice extraction parse failure")
    unrelated = build_token_profile("onedrive download complete")

    assert left
    assert cosine_similarity(left, right) > cosine_similarity(left, unrelated)


def test_sync_semantic_index_persists_intents_incidents_and_docs(tmp_path):
    db = BrixDB(db_path=tmp_path / "semantic.db")
    intent = db.knowledge_entity_add(
        "intent",
        "hmk-semantic-intent",
        "Stabilize HMK parse failures",
        raw_text="Please fix recurring HMK parse failure incidents.",
        project="buddy",
    )
    changelog = db.add_changelog_entry(
        version="14.2.2",
        type="fix",
        title="Fix HMK parse failure handling",
        description="Improves incident diagnosis for HMK parsing.",
        task_id="T-3.2.2",
        commit_sha="deadbeef",
    )
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-hmk-semantic-001",
                "buddy-hmk-extract",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "extract": {
                            "status": "error",
                            "error_message": "HMK parse failure on invoice extraction",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )

    stats = sync_semantic_index(db=db)
    docs = db.semantic_document_list(project="buddy")
    embeddings = db.semantic_embedding_list()

    assert stats["knowledge"] >= 1
    assert stats["incident"] >= 1
    assert stats["docs"] >= 1
    assert any(doc["entity_id"] == intent["id"] for doc in docs)
    assert any(doc["entity_id"] == changelog["id"] for doc in db.semantic_document_list())
    assert any(item["token_count"] > 0 for item in embeddings)


def test_semantic_search_finds_similar_incidents_and_docs(tmp_path):
    db = BrixDB(db_path=tmp_path / "semantic-search.db")
    db.knowledge_entity_add(
        "intent",
        "hmk-search-intent",
        "Resolve HMK parse failures",
        summary="Prior user request about recurring HMK parse failures.",
        project="buddy",
    )
    db.add_changelog_entry(
        version="14.2.2",
        type="fix",
        title="Fix HMK parse failure handling",
        description="Incident repair for invoice extraction parse failure.",
        task_id="T-3.2.2",
        commit_sha="cafebabe",
    )
    with db._connect() as conn:
        conn.execute(
            """
            INSERT INTO run (
                run_id, pipeline, success, started_at, finished_at, duration,
                input_data, steps_data, result_summary, triggered_by, environment_json, project
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                "run-hmk-search-001",
                "buddy-hmk-extract",
                0,
                "2026-04-10T00:00:00+00:00",
                "2026-04-10T00:01:00+00:00",
                60.0,
                "{}",
                json.dumps(
                    {
                        "extract": {
                            "status": "error",
                            "error_message": "HMK parse failure on invoice extraction",
                        }
                    }
                ),
                "",
                "cli",
                "{}",
                "buddy",
            ),
        )

    result = semantic_search("hmk parse failure", db=db, project="buddy", limit=3)

    assert result["strategy"] == "token-cosine-v1"
    assert len(result["matches"]) >= 2
    top_types = {match["document_type"] for match in result["matches"]}
    assert "incident" in top_types
    assert "knowledge" in top_types or "docs" in top_types
    assert result["matches"][0]["score"] >= result["matches"][-1]["score"]


def test_sync_semantic_index_prunes_deleted_sources(tmp_path):
    db = BrixDB(db_path=tmp_path / "semantic-prune.db")
    entry = db.knowledge_entity_add(
        "intent",
        "hmk-prune-intent",
        "Prune deleted semantic sources",
        summary="This entry should disappear from search after deletion.",
        project="buddy",
    )

    sync_semantic_index(db=db)
    before = semantic_search("prune deleted semantic", db=db, project="buddy", limit=5)
    assert any(match["entity_id"] == entry["id"] for match in before["matches"])

    db.knowledge_entity_delete(entry["id"])
    sync_semantic_index(db=db)
    after = semantic_search("prune deleted semantic", db=db, project="buddy", limit=5)
    assert all(match["entity_id"] != entry["id"] for match in after["matches"])
