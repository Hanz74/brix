"""Pipeline-level dependency management.

Provides functions to check and install Python package requirements
declared in a pipeline's ``requirements`` field.
"""
from __future__ import annotations

import importlib.metadata
import subprocess
import sys
from typing import Optional


def _package_name(requirement: str) -> str:
    """Extract the bare package name from a requirement specifier.

    Examples::

        "requests>=2.28"  → "requests"
        "Pillow"          → "Pillow"
        "some-pkg==1.0"   → "some-pkg"
    """
    for sep in (">=", "<=", "!=", "~=", "==", ">", "<", "[", ";"):
        requirement = requirement.split(sep)[0]
    return requirement.strip()


def check_requirements(requirements: list[str]) -> list[str]:
    """Return the subset of *requirements* that are not currently installed.

    Each entry is a PEP-508 requirement specifier (e.g. ``"requests>=2.28"``).
    The function uses :func:`importlib.metadata.version` to check presence;
    version constraints are **not** evaluated — only package existence is tested.

    Parameters
    ----------
    requirements:
        List of requirement specifiers to check.

    Returns
    -------
    list[str]
        Requirements that could not be found in the current environment.
        Empty list means all requirements are satisfied.
    """
    missing: list[str] = []
    for req in requirements:
        pkg = _package_name(req)
        # Normalise: pip uses dashes, importlib.metadata may use underscores
        for candidate in (pkg, pkg.replace("-", "_"), pkg.replace("_", "-")):
            try:
                importlib.metadata.version(candidate)
                break  # found
            except importlib.metadata.PackageNotFoundError:
                continue
        else:
            missing.append(req)
    return missing


def install_requirements(
    requirements: list[str],
    *,
    quiet: bool = True,
    extra_args: Optional[list[str]] = None,
) -> bool:
    """Install *requirements* via ``pip install``.

    Parameters
    ----------
    requirements:
        List of requirement specifiers to install.
    quiet:
        Pass ``--quiet`` to pip (default: True).
    extra_args:
        Additional arguments forwarded to pip (e.g. ``["--user"]``).

    Returns
    -------
    bool
        ``True`` if installation succeeded, ``False`` otherwise.
    """
    if not requirements:
        return True

    cmd = [sys.executable, "-m", "pip", "install"]
    if quiet:
        cmd.append("--quiet")
    if extra_args:
        cmd.extend(extra_args)
    cmd.extend(requirements)

    result = subprocess.run(cmd, capture_output=True, text=True)
    return result.returncode == 0
