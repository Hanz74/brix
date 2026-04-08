"""Tests for helper imports feature (T-BRIX-HELPER-IMPORTS).

Covers:
- PythonRunner materializes imported helpers under their clear name
- Integrity check detects MISSING_IMPORT
- Integrity check detects CIRCULAR_IMPORT
- get_helper response includes imports list
- create_helper / update_helper persist imports
"""
from __future__ import annotations

import hashlib
import json
from pathlib import Path
from types import SimpleNamespace

import pytest

from brix.db import BrixDB
from brix.helper_registry import HelperRegistry
from brix.runners.python import PythonRunner, HELPER_CACHE_DIR
from brix.integrity import run_integrity_checks


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def helper_db(tmp_path: Path) -> BrixDB:
    return BrixDB(db_path=tmp_path / "helpers.db")


@pytest.fixture
def db_registry(helper_db: BrixDB, monkeypatch: pytest.MonkeyPatch) -> HelperRegistry:
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    return HelperRegistry(db=helper_db)


# ---------------------------------------------------------------------------
# PythonRunner: imports materialisation
# ---------------------------------------------------------------------------

@pytest.mark.asyncio
async def test_helper_with_imports_materializes_dependency(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    db_registry: HelperRegistry,
) -> None:
    """When a helper declares imports, the imported helper is written to
    /tmp/brix-helpers/<name>.py so that ``import <name>`` works."""

    # Register the dependency helper
    dep_code = (
        "# shared_utils module\n"
        "def greet(name):\n"
        "    return f'Hello, {name}!'\n"
    )
    db_registry.register(
        name="shared_utils",
        description="Shared utility functions for tests",
        input_schema={},
        output_schema={},
        code=dep_code,
    )

    # Register the main helper that imports shared_utils
    main_code = (
        "import json, sys\n"
        "sys.path.insert(0, __import__('os').environ.get('PYTHONPATH','').split(':')[0])\n"
        "import shared_utils\n"
        "print(json.dumps({'greeting': shared_utils.greet('World')}))\n"
    )
    db_registry.register(
        name="main_helper",
        description="Main helper that imports shared_utils",
        input_schema={},
        output_schema={},
        imports=["shared_utils"],
        code=main_code,
    )

    cache_dir = tmp_path / "cache"
    monkeypatch.setattr("brix.runners.python.HELPER_CACHE_DIR", cache_dir)

    # Patch HelperRegistry inside the helper_registry module so that
    # _materialize_imports (which does a local import) picks up our test DB.
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: db_registry._db)

    step = SimpleNamespace(
        id="s1", helper="main_helper", params={},
        timeout=None, progress=False,
    )
    context = SimpleNamespace(credentials={}, workdir=None)

    result = await PythonRunner().execute(step, context)

    assert result["success"] is True, f"Runner failed: {result.get('error')}"
    assert result["data"]["greeting"] == "Hello, World!"

    # The imported helper should exist under its clear name
    assert (cache_dir / "shared_utils.py").exists()


# ---------------------------------------------------------------------------
# Integrity: MISSING_IMPORT
# ---------------------------------------------------------------------------

def test_missing_import_integrity_issue(helper_db: BrixDB, db_registry: HelperRegistry) -> None:
    """A helper that imports a non-existent helper produces MISSING_IMPORT."""
    db_registry.register(
        name="lonely_helper",
        description="Helper that imports something missing",
        input_schema={},
        output_schema={},
        code="print('hi')\n",
        imports=["does_not_exist"],
    )

    result = run_integrity_checks(helper_db)
    codes = [i["code"] for i in result["issues"]]
    assert "MISSING_IMPORT" in codes

    missing_issue = next(i for i in result["issues"] if i["code"] == "MISSING_IMPORT")
    assert "does_not_exist" in missing_issue["message"]


# ---------------------------------------------------------------------------
# Integrity: CIRCULAR_IMPORT
# ---------------------------------------------------------------------------

def test_circular_import_integrity_issue(helper_db: BrixDB, db_registry: HelperRegistry) -> None:
    """A -> B -> A import cycle produces CIRCULAR_IMPORT."""
    db_registry.register(
        name="helper_a",
        description="Helper A that imports B (test)",
        input_schema={},
        output_schema={},
        code="print('a')\n",
        imports=["helper_b"],
    )
    db_registry.register(
        name="helper_b",
        description="Helper B that imports A (test)",
        input_schema={},
        output_schema={},
        code="print('b')\n",
        imports=["helper_a"],
    )

    result = run_integrity_checks(helper_db)
    codes = [i["code"] for i in result["issues"]]
    assert "CIRCULAR_IMPORT" in codes


# ---------------------------------------------------------------------------
# get_helper returns imports list
# ---------------------------------------------------------------------------

def test_get_helper_shows_imports(db_registry: HelperRegistry) -> None:
    """HelperEntry.imports is populated from the DB."""
    db_registry.register(
        name="importy",
        description="Helper with imports for testing",
        input_schema={},
        output_schema={},
        code="print('x')\n",
        imports=["dep_a", "dep_b"],
    )

    entry = db_registry.get("importy")
    assert entry is not None
    assert entry.imports == ["dep_a", "dep_b"]


# ---------------------------------------------------------------------------
# create_helper / update_helper persist imports
# ---------------------------------------------------------------------------

def test_register_persists_imports(helper_db: BrixDB, db_registry: HelperRegistry) -> None:
    """register() stores imports in the DB."""
    db_registry.register(
        name="with_imports",
        description="Created with imports for test",
        input_schema={},
        output_schema={},
        code="print('hi')\n",
        imports=["foo", "bar"],
    )

    row = helper_db.get_helper("with_imports")
    assert row is not None
    assert row["imports"] == ["foo", "bar"]


def test_update_persists_imports(helper_db: BrixDB, db_registry: HelperRegistry) -> None:
    """update() can change imports."""
    db_registry.register(
        name="updatable",
        description="Helper whose imports get updated",
        input_schema={},
        output_schema={},
        code="print('v1')\n",
        imports=["old_dep"],
    )

    db_registry.update("updatable", imports=["new_dep_a", "new_dep_b"])

    entry = db_registry.get("updatable")
    assert entry is not None
    assert entry.imports == ["new_dep_a", "new_dep_b"]

    row = helper_db.get_helper("updatable")
    assert row["imports"] == ["new_dep_a", "new_dep_b"]
