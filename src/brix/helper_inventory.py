"""DB-first helper inventory and brick-candidate clustering."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any

from brix.db import BrixDB


@dataclass(frozen=True)
class HelperInventoryItem:
    """Strategic classification for one DB-backed helper."""

    name: str
    family: str
    domain: str
    strategic_category: str
    migration_candidacy: str
    used_by_pipelines: tuple[str, ...] = ()
    missing_metadata: tuple[str, ...] = ()
    signals: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "name": self.name,
            "family": self.family,
            "domain": self.domain,
            "strategic_category": self.strategic_category,
            "migration_candidacy": self.migration_candidacy,
            "used_by_pipelines": list(self.used_by_pipelines),
            "usage_count": len(self.used_by_pipelines),
            "missing_metadata": list(self.missing_metadata),
            "signals": list(self.signals),
        }


@dataclass(frozen=True)
class HelperCluster:
    """Helpers grouped by reusable domain family."""

    family: str
    domain: str
    helpers: tuple[str, ...]
    brick_candidates: tuple[str, ...] = ()

    def as_dict(self) -> dict[str, Any]:
        return {
            "family": self.family,
            "domain": self.domain,
            "helpers": list(self.helpers),
            "helper_count": len(self.helpers),
            "brick_candidates": list(self.brick_candidates),
            "brick_candidate_count": len(self.brick_candidates),
        }


@dataclass(frozen=True)
class HelperInventory:
    """Complete helper inventory with item and cluster views."""

    items: tuple[HelperInventoryItem, ...] = ()
    clusters: tuple[HelperCluster, ...] = ()
    summary: dict[str, int] = field(default_factory=dict)

    def as_dict(self) -> dict[str, Any]:
        return {
            "helpers": [item.as_dict() for item in self.items],
            "clusters": [cluster.as_dict() for cluster in self.clusters],
            "summary": dict(self.summary),
        }


_FAMILY_KEYWORDS: tuple[tuple[str, str, tuple[str, ...]], ...] = (
    ("extraction", "extract", ("extract", "parse", "ocr", "markdown", "daigestr", "receipt", "invoice")),
    ("classification", "classification", ("classify", "category", "categorize", "label", "route")),
    ("persistence", "db", ("save", "persist", "upsert", "insert", "update", "database", "postgres", "sql")),
    ("source_transfer", "source", ("download", "fetch", "upload", "onedrive", "outlook", "gmail", "m365")),
    ("conversion", "conversion", ("convert", "normalize", "transform", "base64", "json", "csv", "pdf")),
    ("notification", "notification", ("notify", "mail", "message", "webhook", "mattermost")),
    ("validation", "validation", ("validate", "check", "verify", "guard")),
    ("orchestration", "flow", ("batch", "merge", "split", "dispatch", "schedule")),
)


def _text_for_helper(row: dict[str, Any]) -> str:
    fields = [
        row.get("name", ""),
        row.get("description", ""),
        row.get("script_path", ""),
        " ".join(row.get("tags") or []),
        row.get("code", "")[:4000],
    ]
    return " ".join(str(field).lower() for field in fields if field)


def classify_helper_family(row: dict[str, Any]) -> tuple[str, str, tuple[str, ...]]:
    """Classify a helper into a strategic family using metadata and stored code."""
    text = _text_for_helper(row)
    for family, domain, keywords in _FAMILY_KEYWORDS:
        matched = tuple(keyword for keyword in keywords if keyword in text)
        if matched:
            return family, domain, tuple(f"keyword:{keyword}" for keyword in matched[:5])
    return "utility", "utility", ("fallback:utility",)


def _missing_metadata(row: dict[str, Any]) -> tuple[str, ...]:
    missing: list[str] = []
    if not str(row.get("description") or "").strip():
        missing.append("description")
    if not row.get("input_schema"):
        missing.append("input_schema")
    if not row.get("output_schema"):
        missing.append("output_schema")
    if not row.get("project"):
        missing.append("project")
    if not row.get("tags"):
        missing.append("tags")
    return tuple(missing)


def classify_helper_strategy(
    row: dict[str, Any],
    *,
    family: str,
    usage_count: int,
    missing_metadata: tuple[str, ...],
) -> tuple[str, str, tuple[str, ...]]:
    """Return strategic category, migration candidacy, and signals."""
    signals: list[str] = []
    if missing_metadata:
        signals.append("metadata_incomplete")
    if usage_count > 1:
        signals.append("reused_across_pipelines")
    elif usage_count == 1:
        signals.append("used_by_pipeline")
    else:
        signals.append("unused_in_db_pipelines")
    if not row.get("code"):
        signals.append("no_db_code")
    if row.get("script_path"):
        signals.append("legacy_script_path")
    if family in {"extraction", "classification", "persistence", "source_transfer", "conversion"}:
        signals.append("domain_logic_family")

    if "no_db_code" in signals or "legacy_script_path" in signals:
        return "legacy_review", "review", tuple(signals)
    if usage_count > 1 and "domain_logic_family" in signals:
        return "brick_candidate", "high", tuple(signals)
    if "domain_logic_family" in signals:
        return "brick_candidate", "medium", tuple(signals)
    if missing_metadata:
        return "metadata_required", "review", tuple(signals)
    return "stable_helper", "low", tuple(signals)


def build_helper_inventory(db: BrixDB | None = None) -> HelperInventory:
    """Build a complete DB-first helper inventory with strategic categories."""
    db = db or BrixDB()
    items: list[HelperInventoryItem] = []

    for row in db.list_helpers():
        name = str(row.get("name") or "")
        family, domain, family_signals = classify_helper_family(row)
        used_by = tuple(db.find_pipelines_referencing_helper(name))
        missing = _missing_metadata(row)
        strategic_category, candidacy, strategy_signals = classify_helper_strategy(
            row,
            family=family,
            usage_count=len(used_by),
            missing_metadata=missing,
        )
        items.append(
            HelperInventoryItem(
                name=name,
                family=family,
                domain=domain,
                strategic_category=strategic_category,
                migration_candidacy=candidacy,
                used_by_pipelines=used_by,
                missing_metadata=missing,
                signals=tuple(dict.fromkeys((*family_signals, *strategy_signals))),
            )
        )

    items.sort(key=lambda item: item.name)
    clusters = _build_clusters(items)
    summary: dict[str, int] = {"total": len(items)}
    for item in items:
        summary[item.strategic_category] = summary.get(item.strategic_category, 0) + 1
    return HelperInventory(items=tuple(items), clusters=clusters, summary=summary)


def filter_helper_inventory(inventory: HelperInventory, helper_names: set[str]) -> HelperInventory:
    """Return an inventory view scoped to the supplied helper names."""
    items = tuple(item for item in inventory.items if item.name in helper_names)
    clusters = _build_clusters(list(items))
    summary: dict[str, int] = {"total": len(items)}
    for item in items:
        summary[item.strategic_category] = summary.get(item.strategic_category, 0) + 1
    return HelperInventory(items=items, clusters=clusters, summary=summary)


def _build_clusters(items: list[HelperInventoryItem]) -> tuple[HelperCluster, ...]:
    by_family: dict[str, list[HelperInventoryItem]] = {}
    for item in items:
        by_family.setdefault(item.family, []).append(item)

    clusters: list[HelperCluster] = []
    for family, family_items in sorted(by_family.items()):
        helpers = tuple(item.name for item in sorted(family_items, key=lambda item: item.name))
        candidates = tuple(
            item.name
            for item in sorted(family_items, key=lambda item: item.name)
            if item.strategic_category == "brick_candidate"
        )
        domain = family_items[0].domain if family_items else "utility"
        clusters.append(
            HelperCluster(
                family=family,
                domain=domain,
                helpers=helpers,
                brick_candidates=candidates,
            )
        )
    return tuple(clusters)
