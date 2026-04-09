#!/usr/bin/env python3
from __future__ import annotations

from brix.db import BrixDB
from brix.integrity import run_integrity_checks


REPLACEMENTS = (
    ('"type": "http"', '"type": "http_get"'),
    ('"type": "python"', '"type": "python_script"'),
    ('"type": "mcp"', '"type": "mcp_call"'),
    ('"type": "pipeline"', '"type": "sub_pipeline"'),
)


def main() -> int:
    db = BrixDB()
    topics = db.help_topics_list()
    updated = []

    for topic in topics:
        content = topic.get("content") or ""
        new_content = content
        for old, new in REPLACEMENTS:
            new_content = new_content.replace(old, new)
        if new_content == content:
            continue

        record = dict(topic)
        record["content"] = new_content
        db.help_topics_upsert(record)
        updated.append(topic["name"])

    result = run_integrity_checks(db)
    remaining = [i for i in result["issues"] if i["code"] == "HELP_LEGACY_TYPE"]
    if remaining:
        print("HELP_LEGACY_TYPE still present:")
        for issue in remaining:
            print(issue["message"])
        return 1

    print("updated topics:", ", ".join(updated))
    print("HELP_LEGACY_TYPE resolved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
