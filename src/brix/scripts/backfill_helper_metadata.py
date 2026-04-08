"""Backfill missing helper description/project metadata in the DB.

This script updates existing helper rows via ``HelperRegistry.update()``:
- missing ``project`` is inferred from helper name prefix
- missing ``description`` is derived from the first docstring found in the code
"""
from __future__ import annotations

import argparse
import ast
from dataclasses import dataclass
from typing import Any

from brix.db import BrixDB
from brix.helper_registry import HelperRegistry


PREFIX_PROJECT_MAP: tuple[tuple[str, str], ...] = (
    ("buddy_", "buddy"),
    ("cody_", "cody"),
    ("att_", "buddy"),
    ("dedup_", "utility"),
    ("structured_", "utility"),
)


@dataclass(slots=True)
class BackfillSummary:
    """Counters for a backfill run."""

    scanned: int = 0
    updated: int = 0
    project_backfilled: int = 0
    description_backfilled: int = 0
    skipped: int = 0


def infer_project_from_name(name: str) -> str | None:
    """Infer helper project from the configured name-prefix mapping."""
    normalized = name.strip().lower()
    for prefix, project in PREFIX_PROJECT_MAP:
        if normalized.startswith(prefix):
            return project
    return None


def extract_first_docstring_summary(code: str) -> str | None:
    """Return a one-line summary from the first docstring found in code."""
    if not code.strip():
        return None

    try:
        module = ast.parse(code)
    except SyntaxError:
        return None

    def _iter_docstring_nodes(node: ast.AST) -> list[ast.AST]:
        if isinstance(node, ast.Module):
            nested = node.body
        elif isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            nested = node.body
        else:
            nested = []

        nodes: list[ast.AST] = [node]
        for child in nested:
            if isinstance(child, (ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
                nodes.extend(_iter_docstring_nodes(child))
        return nodes

    for node in _iter_docstring_nodes(module):
        if not isinstance(node, (ast.Module, ast.FunctionDef, ast.AsyncFunctionDef, ast.ClassDef)):
            continue
        docstring = ast.get_docstring(node)
        if not docstring:
            continue
        summary = " ".join(line.strip() for line in docstring.splitlines() if line.strip()).strip()
        if summary:
            return summary
    return None


def build_updates(row: dict[str, Any]) -> dict[str, Any]:
    """Return the metadata fields that should be backfilled for one helper."""
    updates: dict[str, Any] = {}

    project = (row.get("project") or "").strip()
    if not project:
        inferred_project = infer_project_from_name(row["name"])
        if inferred_project is not None:
            updates["project"] = inferred_project

    description = (row.get("description") or "").strip()
    if not description:
        derived_description = extract_first_docstring_summary(row.get("code") or "")
        if derived_description is not None:
            updates["description"] = derived_description

    return updates


def run_backfill(*, dry_run: bool = False) -> BackfillSummary:
    """Backfill missing helper metadata for all helpers in the DB."""
    db = BrixDB()
    registry = HelperRegistry()
    rows = db.list_helpers()

    summary = BackfillSummary(scanned=len(rows))
    for row in rows:
        updates = build_updates(row)
        if not updates:
            summary.skipped += 1
            continue

        if not dry_run:
            registry.update(row["name"], **updates)

        summary.updated += 1
        if "project" in updates:
            summary.project_backfilled += 1
        if "description" in updates:
            summary.description_backfilled += 1

    return summary


def main() -> int:
    """Run the helper metadata backfill."""
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--dry-run", action="store_true", help="Report changes without writing updates")
    args = parser.parse_args()

    summary = run_backfill(dry_run=args.dry_run)
    mode = "dry-run" if args.dry_run else "updated"
    print(
        f"helper_metadata_backfill: mode={mode} scanned={summary.scanned} updated={summary.updated} "
        f"project_backfilled={summary.project_backfilled} description_backfilled={summary.description_backfilled} "
        f"skipped={summary.skipped}"
    )
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
