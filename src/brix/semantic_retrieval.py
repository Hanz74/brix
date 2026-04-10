from __future__ import annotations

import json
import math
import re
from collections import Counter
from typing import Any, Optional

from brix.db import BrixDB

_STRATEGY = "token-cosine-v1"
_TOKEN_RE = re.compile(r"[a-z0-9]{3,}")
_STOP_WORDS = {
    "the",
    "and",
    "for",
    "with",
    "that",
    "this",
    "from",
    "have",
    "been",
    "error",
    "failure",
    "into",
    "after",
    "when",
    "will",
    "your",
    "about",
    "buddy",
    "brix",
}


def _tokenize(text: str) -> list[str]:
    return [
        token
        for token in _TOKEN_RE.findall((text or "").lower())
        if token not in _STOP_WORDS
    ]


def build_token_profile(text: str) -> dict[str, float]:
    tokens = _tokenize(text)
    if not tokens:
        return {}
    counts = Counter(tokens)
    total = float(sum(counts.values()))
    return {
        token: round(count / total, 6)
        for token, count in counts.items()
    }


def cosine_similarity(left: dict[str, float], right: dict[str, float]) -> float:
    if not left or not right:
        return 0.0
    dot = sum(left.get(token, 0.0) * right.get(token, 0.0) for token in set(left) | set(right))
    left_norm = math.sqrt(sum(value * value for value in left.values()))
    right_norm = math.sqrt(sum(value * value for value in right.values()))
    if left_norm == 0.0 or right_norm == 0.0:
        return 0.0
    return dot / (left_norm * right_norm)


def sync_semantic_index(db: Optional[BrixDB] = None) -> dict[str, int]:
    """Persist semantic documents + token profiles for intents, incidents, and docs."""
    db = db or BrixDB()
    counts = {
        "knowledge": 0,
        "incident": 0,
        "docs": 0,
    }
    desired_keys: set[tuple[str, str, str]] = set()

    for entity in db.knowledge_entity_list():
        text = "\n".join(
            [
                entity.get("title", ""),
                entity.get("raw_text", ""),
                entity.get("summary", ""),
                entity.get("rationale", ""),
                json.dumps(entity.get("content", {}), sort_keys=True),
            ]
        ).strip()
        if not text:
            continue
        doc = db.semantic_document_upsert(
            entity_type=entity["entity_type"],
            entity_id=entity["id"],
            document_type="knowledge",
            title=entity.get("title", ""),
            text_content=text,
            project=entity.get("project", ""),
            metadata={"name": entity.get("name", ""), "tags": entity.get("tags", [])},
        )
        db.semantic_embedding_upsert(
            doc["id"],
            strategy=_STRATEGY,
            token_weights=build_token_profile(text),
        )
        desired_keys.add((doc["entity_type"], doc["entity_id"], doc["document_type"]))
        counts["knowledge"] += 1

    with db._connect() as conn:
        conn.row_factory = None
        changelog_rows = conn.execute(
            "SELECT id, version, type, title, description, task_id, commit_sha FROM changelog_entry"
        ).fetchall()
        run_rows = conn.execute(
            "SELECT run_id, pipeline, project, steps_data FROM run WHERE steps_data IS NOT NULL"
        ).fetchall()

    for row in changelog_rows:
        entry_id, version, entry_type, title, description, task_id, commit_sha = row
        text = "\n".join(
            [
                str(title or ""),
                str(description or ""),
                str(task_id or ""),
                str(commit_sha or ""),
                str(version or ""),
                str(entry_type or ""),
            ]
        ).strip()
        if not text:
            continue
        doc = db.semantic_document_upsert(
            entity_type="changelog",
            entity_id=entry_id,
            document_type="docs",
            title=str(title or ""),
            text_content=text,
            metadata={
                "version": version or "",
                "type": entry_type or "",
                "task_id": task_id or "",
            },
        )
        db.semantic_embedding_upsert(
            doc["id"],
            strategy=_STRATEGY,
            token_weights=build_token_profile(text),
        )
        desired_keys.add((doc["entity_type"], doc["entity_id"], doc["document_type"]))
        counts["docs"] += 1

    for run_id, pipeline, project, steps_data in run_rows:
        try:
            steps = json.loads(steps_data or "{}")
        except (json.JSONDecodeError, TypeError):
            continue
        for step_id, data in steps.items():
            status = str(data.get("status") or "")
            error_message = str(data.get("error_message") or data.get("errors") or "")
            text = "\n".join(
                [
                    str(pipeline or ""),
                    step_id,
                    status,
                    error_message,
                ]
            ).strip()
            if not text:
                continue
            finding_id = f"{run_id}:{step_id}"
            doc = db.semantic_document_upsert(
                entity_type="finding",
                entity_id=finding_id,
                document_type="incident",
                title=f"{pipeline}:{step_id}",
                text_content=text,
                project=str(project or ""),
                metadata={"run_id": run_id, "pipeline": pipeline or "", "status": status},
            )
            db.semantic_embedding_upsert(
                doc["id"],
                strategy=_STRATEGY,
                token_weights=build_token_profile(text),
            )
            desired_keys.add((doc["entity_type"], doc["entity_id"], doc["document_type"]))
            counts["incident"] += 1

    indexed_docs = db.semantic_document_list(document_types=["knowledge", "docs", "incident"])
    stale_ids = [
        doc["id"]
        for doc in indexed_docs
        if (doc["entity_type"], doc["entity_id"], doc["document_type"]) not in desired_keys
    ]
    if stale_ids:
        placeholders = ",".join("?" * len(stale_ids))
        with db._connect() as conn:
            conn.execute(
                f"DELETE FROM semantic_embedding WHERE document_id IN ({placeholders})",
                stale_ids,
            )
            conn.execute(
                f"DELETE FROM semantic_document WHERE id IN ({placeholders})",
                stale_ids,
            )

    return counts


def semantic_search(
    query: str,
    *,
    db: Optional[BrixDB] = None,
    entity_types: Optional[list[str]] = None,
    project: Optional[str] = None,
    limit: int = 5,
) -> dict[str, Any]:
    """Search intents, incidents, and docs via persisted token profiles."""
    db = db or BrixDB()
    sync_stats = sync_semantic_index(db=db)
    query_profile = build_token_profile(query)

    docs = db.semantic_document_list(entity_types=entity_types, project=project)
    embeddings = {item["document_id"]: item for item in db.semantic_embedding_list()}
    query_lower = (query or "").lower().strip()

    matches: list[dict[str, Any]] = []
    for doc in docs:
        embedding = embeddings.get(doc["id"])
        if embedding is None:
            continue
        score = cosine_similarity(query_profile, embedding.get("token_weights", {}))
        if query_lower and query_lower in (doc.get("title", "").lower() + " " + doc.get("text_content", "").lower()):
            score += 0.2
        if score <= 0.0:
            continue
        matches.append(
            {
                "document_id": doc["id"],
                "entity_type": doc["entity_type"],
                "entity_id": doc["entity_id"],
                "document_type": doc["document_type"],
                "title": doc.get("title", ""),
                "project": doc.get("project", ""),
                "metadata": doc.get("metadata", {}),
                "score": round(score, 6),
                "strategy": embedding.get("strategy", _STRATEGY),
            }
        )

    matches.sort(key=lambda item: item["score"], reverse=True)
    return {
        "query": query,
        "strategy": _STRATEGY,
        "sync_stats": sync_stats,
        "matches": matches[:limit],
    }
