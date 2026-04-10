"""Policy for file mirrors in the DB-first Brix architecture.

Files may represent exports, bundles, backups, debug artifacts, or legacy
import sources. They are never authoritative authoring state.
"""
from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum


class FileMirrorPurpose(StrEnum):
    """Allowed non-authoritative reasons for a Brix-managed file."""

    EXPORT = "export"
    BACKUP = "backup"
    BUNDLE = "bundle"
    DEBUG = "debug"
    LEGACY_IMPORT = "legacy_import"


@dataclass(frozen=True)
class FileMirrorPolicy:
    """Describes the invariant boundary between DB truth and file artifacts."""

    authoritative_authoring_truth: str = "db"
    files_are_authoritative: bool = False
    allowed_purposes: tuple[FileMirrorPurpose, ...] = tuple(FileMirrorPurpose)
    forbidden_use: str = "Do not treat file mirrors as primary persistence or repair truth."


FILE_MIRROR_POLICY = FileMirrorPolicy()


def is_authoritative_file_purpose(purpose: str | FileMirrorPurpose) -> bool:
    """Return False for all valid file purposes because files are mirror artifacts."""
    try:
        FileMirrorPurpose(purpose)
    except ValueError as exc:
        raise ValueError(f"unknown file mirror purpose: {purpose}") from exc
    return False


def allowed_file_mirror_purposes() -> tuple[str, ...]:
    """Return allowed file mirror purposes as stable API strings."""
    return tuple(purpose.value for purpose in FILE_MIRROR_POLICY.allowed_purposes)
