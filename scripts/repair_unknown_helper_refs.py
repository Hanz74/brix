#!/usr/bin/env python3
from __future__ import annotations

import argparse
import json
import sqlite3
from pathlib import Path

from brix.db import BRIX_DB_PATH, BrixDB
from brix.integrity import run_integrity_checks


RESTORE_FROM_BACKUP = (
    "apply_template_updates",
    "buddy_extract_structured_llm",
    "buddy_extract_merge_validate",
    "att_find_candidates",
    "att_process_single",
    "enrich_markitdown_templates",
    "import_templates_to_markitdown",
)

FILE_TO_BASE64_CODE = """#!/usr/bin/env python3
import base64
import json
import sys
from pathlib import Path


def _load_payload() -> dict:
    if len(sys.argv) > 1 and sys.argv[1]:
        return json.loads(sys.argv[1])
    raw = sys.stdin.read().strip()
    return json.loads(raw) if raw else {}


def main() -> None:
    payload = _load_payload()
    path = Path(payload["path"])
    data = path.read_bytes()
    print(json.dumps({
        "path": str(path),
        "filename": path.name,
        "base64": base64.b64encode(data).decode("ascii"),
        "size_bytes": len(data),
    }))


if __name__ == "__main__":
    main()
"""


def _load_backup_helper_rows(backup_db: Path) -> dict[str, dict]:
    conn = sqlite3.connect(str(backup_db))
    conn.row_factory = sqlite3.Row
    try:
        rows: dict[str, dict] = {}
        for name in RESTORE_FROM_BACKUP:
            row = conn.execute("SELECT * FROM helper WHERE name=?", (name,)).fetchone()
            if row is None:
                raise SystemExit(f"missing helper '{name}' in backup {backup_db}")
            rows[name] = dict(row)
        return rows
    finally:
        conn.close()


def _restore_backup_helper(target_db: BrixDB, row: dict) -> None:
    target_db.upsert_helper(
        name=row["name"],
        script_path=row.get("script_path") or "",
        description=row.get("description") or "",
        requirements=json.loads(row.get("requirements_json") or "[]"),
        input_schema=json.loads(row.get("input_schema_json") or "{}"),
        output_schema=json.loads(row.get("output_schema_json") or "{}"),
        code=row.get("code") or "",
        content_hash=row.get("content_hash") or "",
        project=row.get("project") or None,
        tags=json.loads(row.get("tags") or "[]"),
        group_name=row.get("group_name") or None,
        imports=json.loads(row.get("imports_json") or "[]"),
        helper_id=row.get("id"),
    )


def _restore_file_to_base64(target_db: BrixDB) -> None:
    target_db.upsert_helper(
        name="file_to_base64",
        script_path="",
        description="Read a file from disk and return filename plus base64 content as JSON.",
        requirements=[],
        input_schema={},
        output_schema={},
        code=FILE_TO_BASE64_CODE,
        project="utility",
        tags=["conversion", "one-shot"],
    )


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Restore deleted helpers that current pipeline_step rows still reference.",
    )
    parser.add_argument(
        "--target-db",
        type=Path,
        default=BRIX_DB_PATH,
        help=f"Target brix.db path (default: {BRIX_DB_PATH})",
    )
    parser.add_argument(
        "--backup-db",
        type=Path,
        default=Path("/root/.brix/backups/brix-backup-brix-2026-04-08T13-05-59.db"),
        help="Backup DB path that still contains the deleted helper rows.",
    )
    args = parser.parse_args()

    backup_rows = _load_backup_helper_rows(args.backup_db)
    target_db = BrixDB(db_path=args.target_db)

    for name in RESTORE_FROM_BACKUP:
        _restore_backup_helper(target_db, backup_rows[name])
        print(f"restored helper: {name}")

    _restore_file_to_base64(target_db)
    print("restored helper: file_to_base64")

    result = run_integrity_checks(target_db)
    helper_issues = [i for i in result["issues"] if i["code"] == "UNKNOWN_HELPER_REF"]
    if helper_issues:
        print("UNKNOWN_HELPER_REF still present:")
        for issue in helper_issues:
            print(issue["message"])
        return 1

    print("UNKNOWN_HELPER_REF resolved.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
