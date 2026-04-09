"""Changelog handler module — T-BRIX-CHANGELOG-01."""
from __future__ import annotations

from collections import defaultdict

from brix.db import BrixDB, _parse_semver


async def _handle_changelog(arguments: dict) -> dict:
    """List changelog entries grouped by version.

    Parameters (all optional):
        since: Only return entries for versions >= this semver string.
        type:  Filter by entry type (breaking/feature/fix/refactor/docs).
        limit: Maximum number of entries (default 50).
    """
    since = arguments.get("since") or None
    entry_type = arguments.get("type") or None
    limit = int(arguments.get("limit", 50))

    db = BrixDB()
    entries = db.list_changelog(since=since, type=entry_type, limit=limit)

    # Group by version
    by_version: dict[str, list[dict]] = defaultdict(list)
    for e in entries:
        by_version[e["version"]].append({
            "type": e["type"],
            "title": e["title"],
            "description": e.get("description", ""),
            "commit_sha": e.get("commit_sha"),
            "task_id": e.get("task_id"),
            "timestamp": e["timestamp"],
        })

    # Sort versions descending using semantic version order.
    versions = sorted(by_version.keys(), key=_parse_semver, reverse=True)
    result = [
        {"version": v, "entries": by_version[v]}
        for v in versions
    ]

    return {
        "versions": result,
        "total_entries": len(entries),
    }
