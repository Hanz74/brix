"""Git hook helpers for auto-bumping the package version."""

from __future__ import annotations

import re
import sys
from pathlib import Path

PYPROJECT_PATH = Path("pyproject.toml")
INIT_PATH = Path("src/brix/__init__.py")

_VERSION_RE = re.compile(r'^version = "(\d+)\.(\d+)\.(\d+)"$', re.MULTILINE)
_INIT_VERSION_RE = re.compile(r'^__version__ = "(\d+)\.(\d+)\.(\d+)"$', re.MULTILINE)
_BREAKING_PREFIX_RE = re.compile(r"^(?:feat|fix)(?:\([^)]+\))?!:", re.IGNORECASE)
_MINOR_PREFIX_RE = re.compile(r"^(?:feat)(?:\([^)]+\))?:", re.IGNORECASE)
_TICKET_MINOR_PREFIX_RE = re.compile(r"^T-BRIX-V[0-9]+-[0-9]+:", re.IGNORECASE)
_BREAKING_CHANGE_RE = re.compile(r"(^|\n)BREAKING CHANGE:", re.IGNORECASE)


def classify_bump(message: str) -> str:
    """Return the semver bump type implied by a commit message."""
    normalized = message.strip()
    if not normalized:
        return "patch"

    subject = normalized.splitlines()[0].strip()
    if _BREAKING_PREFIX_RE.match(subject) or _BREAKING_CHANGE_RE.search(normalized):
        return "major"
    if _MINOR_PREFIX_RE.match(subject) or _TICKET_MINOR_PREFIX_RE.match(subject):
        return "minor"
    return "patch"


def bump_version(current: str, bump: str) -> str:
    major, minor, patch = (int(part) for part in current.split("."))
    if bump == "major":
        return f"{major + 1}.0.0"
    if bump == "minor":
        return f"{major}.{minor + 1}.0"
    if bump == "patch":
        return f"{major}.{minor}.{patch + 1}"
    raise ValueError(f"Unsupported bump type: {bump}")


def read_pyproject_version(pyproject_path: Path = PYPROJECT_PATH) -> str | None:
    if not pyproject_path.exists():
        return None
    match = _VERSION_RE.search(pyproject_path.read_text())
    if not match:
        return None
    return ".".join(match.groups())


def write_versions(
    new_version: str,
    *,
    pyproject_path: Path = PYPROJECT_PATH,
    init_path: Path = INIT_PATH,
) -> tuple[str, str] | None:
    """Update both version locations and return (old_version, new_version)."""
    if not pyproject_path.exists():
        return None

    pyproject_text = pyproject_path.read_text()
    pyproject_match = _VERSION_RE.search(pyproject_text)
    if not pyproject_match:
        return None
    old_version = ".".join(pyproject_match.groups())
    if old_version == new_version:
        return (old_version, new_version)

    pyproject_path.write_text(
        _VERSION_RE.sub(f'version = "{new_version}"', pyproject_text, count=1)
    )

    if init_path.exists():
        init_text = init_path.read_text()
        init_path.write_text(
            _INIT_VERSION_RE.sub(f'__version__ = "{new_version}"', init_text, count=1)
        )

    return (old_version, new_version)


def apply_commit_bump(
    message: str,
    *,
    pyproject_path: Path = PYPROJECT_PATH,
    init_path: Path = INIT_PATH,
) -> tuple[str, str, str] | None:
    """Classify and apply the version bump for a commit message."""
    current = read_pyproject_version(pyproject_path)
    if not current:
        return None

    bump = classify_bump(message)
    new_version = bump_version(current, bump)
    written = write_versions(new_version, pyproject_path=pyproject_path, init_path=init_path)
    if not written:
        return None

    old_version, _ = written
    return (bump, old_version, new_version)


def main(argv: list[str] | None = None) -> int:
    args = list(sys.argv[1:] if argv is None else argv)
    if not args:
        return 0

    msg_file = Path(args[0])
    pyproject_path = Path(args[1]) if len(args) > 1 else PYPROJECT_PATH
    init_path = Path(args[2]) if len(args) > 2 else INIT_PATH
    result = apply_commit_bump(
        msg_file.read_text(),
        pyproject_path=pyproject_path,
        init_path=init_path,
    )
    if result:
        bump, old_version, new_version = result
        print(f"{bump}|{old_version}|{new_version}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
