"""Pipeline Import/Export — bundle a pipeline with its helper scripts.

A bundle is a gzip-compressed tar archive (``.brix.tar.gz``) with the
following layout::

    pipeline.yaml          # the pipeline definition (always at root)
    helpers/               # all helper scripts referenced by the pipeline
        flatten_*.py
        save_*.py
        ...
    manifest.json          # metadata: brix_version, created_at, checksum

Export
------
``brix bundle export pipeline.yaml --output bundle.tar.gz``

Scans the pipeline YAML for ``script: helpers/<name>.py`` references,
collects every referenced helper, and writes the archive.

Import
------
``brix bundle import bundle.tar.gz [--pipelines-dir DIR] [--helpers-dir DIR]``

Extracts the archive and places files in the right locations.  By default:
- Pipeline YAML → ``~/.brix/pipelines/``
- Helpers       → the helpers directory detected from the existing
                  ``helpers/`` path next to the pipelines dir, or
                  ``/app/helpers`` inside the container.
"""
from __future__ import annotations

import hashlib
import json
import re
import tarfile
import tempfile
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from typing import Optional

import yaml

from brix import __version__

# ---------------------------------------------------------------------------
# Constants
# ---------------------------------------------------------------------------

BUNDLE_SUFFIX = ".brix.tar.gz"
MANIFEST_NAME = "manifest.json"
PIPELINE_NAME_IN_BUNDLE = "pipeline.yaml"
HELPERS_DIR_IN_BUNDLE = "helpers"

# Default target locations (mirrors PipelineStore defaults)
DEFAULT_PIPELINES_DIR = Path.home() / ".brix" / "pipelines"
DEFAULT_HELPERS_DIRS: list[Path] = [
    Path("/app/helpers"),
    Path.home() / ".brix" / "helpers",
]


# ---------------------------------------------------------------------------
# Helper discovery
# ---------------------------------------------------------------------------

_HELPER_PATTERN = re.compile(
    r"""
    (?:
        script\s*:\s*          # YAML key
        |                       # OR
        helpers/               # bare path prefix
    )
    (helpers/[\w./-]+\.py)     # capture path like helpers/foo.py
    """,
    re.VERBOSE,
)


def find_helper_references(pipeline_yaml: str) -> list[str]:
    """Return sorted list of ``helpers/<name>.py`` paths found in pipeline YAML.

    Scans both the ``script:`` field values and any bare ``helpers/`` mentions
    in param values.
    """
    refs: set[str] = set()

    # Parse YAML and walk all string values recursively
    try:
        data = yaml.safe_load(pipeline_yaml)
        _collect_strings(data, refs)
    except Exception:
        pass

    # Also regex scan for safety (catches jinja-embedded references)
    for match in _HELPER_PATTERN.finditer(pipeline_yaml):
        refs.add(match.group(1))

    return sorted(refs)


def _collect_strings(node, refs: set[str]) -> None:
    """Recursively walk YAML-parsed structure and collect helper references."""
    if isinstance(node, str):
        if node.startswith("helpers/") and node.endswith(".py"):
            refs.add(node)
    elif isinstance(node, dict):
        for v in node.values():
            _collect_strings(v, refs)
    elif isinstance(node, list):
        for item in node:
            _collect_strings(item, refs)


# ---------------------------------------------------------------------------
# Export
# ---------------------------------------------------------------------------

def export_bundle(
    pipeline_path: Path,
    output_path: Path,
    *,
    base_dir: Optional[Path] = None,
    include_missing: bool = False,
) -> "BundleManifest":
    """Create a ``.brix.tar.gz`` bundle from a pipeline and its helpers.

    Parameters
    ----------
    pipeline_path:
        Path to the pipeline YAML file.
    output_path:
        Destination archive path (will be created/overwritten).
    base_dir:
        Directory used to resolve relative ``helpers/`` paths.
        Defaults to the pipeline file's parent directory.
    include_missing:
        If ``True``, skip missing helper files with a warning.
        If ``False`` (default), raise ``FileNotFoundError``.

    Returns
    -------
    BundleManifest
        Metadata about the created bundle.
    """
    pipeline_path = pipeline_path.resolve()
    if base_dir is None:
        base_dir = pipeline_path.parent

    pipeline_yaml = pipeline_path.read_text(encoding="utf-8")
    helper_refs = find_helper_references(pipeline_yaml)

    # Resolve helpers — search order:
    # 1. base_dir / ref  (pipeline's own directory)
    # 2. ~/.brix/helpers/<name>  (managed helper storage)
    # 3. /app/helpers/<name>     (legacy container location)
    _managed_helpers = Path.home() / ".brix" / "helpers"
    _legacy_helpers = Path("/app/helpers")

    helpers: list[tuple[str, Path]] = []  # (archive_path, local_path)
    missing: list[str] = []
    for ref in helper_refs:
        local = base_dir / ref
        if local.exists():
            helpers.append((ref, local))
            continue

        # Fallback: search by filename in managed and legacy dirs
        script_name = Path(ref).name
        managed_candidate = _managed_helpers / script_name
        legacy_candidate = _legacy_helpers / script_name
        if managed_candidate.exists():
            helpers.append((ref, managed_candidate))
        elif legacy_candidate.exists():
            helpers.append((ref, legacy_candidate))
        else:
            if include_missing:
                missing.append(ref)
            else:
                raise FileNotFoundError(
                    f"Helper script not found: {local}\n"
                    f"  (also searched: {managed_candidate}, {legacy_candidate})\n"
                    f"  (referenced in pipeline as '{ref}')\n"
                    f"  Use --include-missing to skip missing helpers."
                )

    # Extract requirements from pipeline (T-BRIX-V4-BUG-11)
    from brix.loader import PipelineLoader as _PipelineLoader
    try:
        _loader = _PipelineLoader()
        _pipeline = _loader.load(str(pipeline_path))
        _requirements = list(_pipeline.requirements)
    except Exception:
        _requirements = []

    # Build manifest
    manifest = BundleManifest(
        pipeline_name=_extract_pipeline_name(pipeline_yaml),
        pipeline_file=pipeline_path.name,
        brix_version=__version__,
        created_at=datetime.now(tz=timezone.utc).isoformat(),
        helpers=[ref for ref, _ in helpers],
        missing_helpers=missing,
        pipeline_checksum=_sha256(pipeline_yaml.encode()),
        requirements=_requirements,
    )

    # Write archive
    output_path = output_path.resolve()
    output_path.parent.mkdir(parents=True, exist_ok=True)

    with tarfile.open(output_path, "w:gz") as tar:
        # 1. Pipeline YAML
        _tar_add_bytes(
            tar,
            data=pipeline_yaml.encode("utf-8"),
            arcname=PIPELINE_NAME_IN_BUNDLE,
        )

        # 2. Helpers
        for arcname, local_path in helpers:
            tar.add(str(local_path), arcname=arcname)

        # 3. Manifest
        manifest_json = json.dumps(manifest.to_dict(), indent=2).encode("utf-8")
        _tar_add_bytes(tar, data=manifest_json, arcname=MANIFEST_NAME)

    return manifest


def _tar_add_bytes(tar: tarfile.TarFile, *, data: bytes, arcname: str) -> None:
    """Add raw bytes to a tar archive under ``arcname``."""
    info = tarfile.TarInfo(name=arcname)
    info.size = len(data)
    tar.addfile(info, BytesIO(data))


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _extract_pipeline_name(yaml_text: str) -> str:
    try:
        data = yaml.safe_load(yaml_text) or {}
        return data.get("name", "unknown")
    except Exception:
        return "unknown"


# ---------------------------------------------------------------------------
# Import
# ---------------------------------------------------------------------------

def import_bundle(
    bundle_path: Path,
    *,
    pipelines_dir: Optional[Path] = None,
    helpers_dir: Optional[Path] = None,
    overwrite: bool = False,
) -> "ImportResult":
    """Extract a ``.brix.tar.gz`` bundle and install pipeline + helpers.

    Parameters
    ----------
    bundle_path:
        Path to the ``.brix.tar.gz`` file.
    pipelines_dir:
        Where to write the pipeline YAML.  Defaults to ``~/.brix/pipelines/``.
    helpers_dir:
        Where to write helper scripts.  Defaults to ``/app/helpers`` (first
        existing dir in ``DEFAULT_HELPERS_DIRS``).
    overwrite:
        Overwrite existing files without raising an error.

    Returns
    -------
    ImportResult
        Summary of what was installed.
    """
    bundle_path = bundle_path.resolve()

    if pipelines_dir is None:
        pipelines_dir = DEFAULT_PIPELINES_DIR
    pipelines_dir = Path(pipelines_dir)
    pipelines_dir.mkdir(parents=True, exist_ok=True)

    if helpers_dir is None:
        helpers_dir = _find_helpers_dir()
    helpers_dir = Path(helpers_dir)
    helpers_dir.mkdir(parents=True, exist_ok=True)

    installed_pipeline: Optional[Path] = None
    installed_helpers: list[Path] = []
    manifest: Optional[BundleManifest] = None

    with tarfile.open(bundle_path, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers() if not m.isdir()}

        # Read manifest first (if present)
        if MANIFEST_NAME in members:
            f = tar.extractfile(members[MANIFEST_NAME])
            if f:
                manifest = BundleManifest.from_dict(json.loads(f.read()))

        # Extract pipeline YAML
        if PIPELINE_NAME_IN_BUNDLE not in members:
            raise ValueError(
                f"Invalid bundle: '{PIPELINE_NAME_IN_BUNDLE}' not found in archive."
            )

        pipeline_member = members[PIPELINE_NAME_IN_BUNDLE]
        f = tar.extractfile(pipeline_member)
        if not f:
            raise ValueError("Cannot read pipeline.yaml from bundle.")
        pipeline_yaml = f.read().decode("utf-8")

        # Determine output filename
        pipeline_name = _extract_pipeline_name(pipeline_yaml)
        dest_pipeline = pipelines_dir / f"{pipeline_name}.yaml"

        if dest_pipeline.exists() and not overwrite:
            raise FileExistsError(
                f"Pipeline already exists: {dest_pipeline}\n"
                f"  Use --overwrite to replace it."
            )
        dest_pipeline.write_text(pipeline_yaml, encoding="utf-8")
        installed_pipeline = dest_pipeline

        # Extract helpers
        for name, member in members.items():
            if name.startswith(f"{HELPERS_DIR_IN_BUNDLE}/") and name.endswith(".py"):
                helper_name = Path(name).name
                dest_helper = helpers_dir / helper_name

                if dest_helper.exists() and not overwrite:
                    raise FileExistsError(
                        f"Helper already exists: {dest_helper}\n"
                        f"  Use --overwrite to replace it."
                    )

                f = tar.extractfile(member)
                if f:
                    dest_helper.write_bytes(f.read())
                    installed_helpers.append(dest_helper)

    # Auto-install requirements declared in the manifest (T-BRIX-V4-BUG-11)
    installed_requirements: list[str] = []
    failed_requirements: list[str] = []
    if manifest and manifest.requirements:
        from brix.deps import check_requirements, install_requirements
        missing_reqs = check_requirements(manifest.requirements)
        if missing_reqs:
            ok = install_requirements(missing_reqs)
            if ok:
                installed_requirements = missing_reqs
            else:
                failed_requirements = missing_reqs

    return ImportResult(
        pipeline=installed_pipeline,
        helpers=installed_helpers,
        manifest=manifest,
        installed_requirements=installed_requirements,
        failed_requirements=failed_requirements,
    )


def _find_helpers_dir() -> Path:
    """Return the managed helpers directory (~/.brix/helpers/), falling back to legacy.

    The managed path (``~/.brix/helpers/``) is always preferred.  The legacy
    ``/app/helpers`` is used as a last resort for backward compatibility.
    """
    managed = Path.home() / ".brix" / "helpers"
    # Prefer managed storage — create it if needed
    managed.mkdir(parents=True, exist_ok=True)
    return managed


# ---------------------------------------------------------------------------
# Read manifest from existing bundle (without extracting)
# ---------------------------------------------------------------------------

def read_manifest(bundle_path: Path) -> Optional["BundleManifest"]:
    """Read the manifest from a bundle without extracting it."""
    with tarfile.open(bundle_path, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
        if MANIFEST_NAME not in members:
            return None
        f = tar.extractfile(members[MANIFEST_NAME])
        if not f:
            return None
        return BundleManifest.from_dict(json.loads(f.read()))


# ---------------------------------------------------------------------------
# Data classes
# ---------------------------------------------------------------------------

class BundleManifest:
    """Metadata stored in the bundle's ``manifest.json``."""

    def __init__(
        self,
        *,
        pipeline_name: str,
        pipeline_file: str,
        brix_version: str,
        created_at: str,
        helpers: list[str],
        missing_helpers: list[str] | None = None,
        pipeline_checksum: str = "",
        requirements: list[str] | None = None,
    ) -> None:
        self.pipeline_name = pipeline_name
        self.pipeline_file = pipeline_file
        self.brix_version = brix_version
        self.created_at = created_at
        self.helpers = helpers
        self.missing_helpers = missing_helpers or []
        self.pipeline_checksum = pipeline_checksum
        self.requirements = requirements or []

    def to_dict(self) -> dict:
        return {
            "pipeline_name": self.pipeline_name,
            "pipeline_file": self.pipeline_file,
            "brix_version": self.brix_version,
            "created_at": self.created_at,
            "helpers": self.helpers,
            "missing_helpers": self.missing_helpers,
            "pipeline_checksum": self.pipeline_checksum,
            "requirements": self.requirements,
        }

    @classmethod
    def from_dict(cls, data: dict) -> "BundleManifest":
        return cls(
            pipeline_name=data.get("pipeline_name", "unknown"),
            pipeline_file=data.get("pipeline_file", "pipeline.yaml"),
            brix_version=data.get("brix_version", "?"),
            created_at=data.get("created_at", ""),
            helpers=data.get("helpers", []),
            missing_helpers=data.get("missing_helpers", []),
            pipeline_checksum=data.get("pipeline_checksum", ""),
            requirements=data.get("requirements", []),
        )


class ImportResult:
    """Result of a bundle import operation."""

    def __init__(
        self,
        *,
        pipeline: Optional[Path],
        helpers: list[Path],
        manifest: Optional[BundleManifest],
        installed_requirements: list[str] | None = None,
        failed_requirements: list[str] | None = None,
    ) -> None:
        self.pipeline = pipeline
        self.helpers = helpers
        self.manifest = manifest
        self.installed_requirements: list[str] = installed_requirements or []
        self.failed_requirements: list[str] = failed_requirements or []
