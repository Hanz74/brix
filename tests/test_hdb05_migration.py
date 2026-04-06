from __future__ import annotations

import hashlib
import logging
from pathlib import Path

from brix.db import BrixDB
from brix.migrations import _import_legacy_helper_code_v76


def _make_dirs(tmp_path: Path) -> tuple[Path, Path]:
    legacy_dir = tmp_path / "app_helpers"
    managed_dir = tmp_path / "managed_helpers"
    legacy_dir.mkdir()
    managed_dir.mkdir()
    return legacy_dir, managed_dir


def test_migration_fills_empty_code_column_from_disk(tmp_path, caplog) -> None:
    db = BrixDB(db_path=tmp_path / "hdb05-fill.db")
    legacy_dir, managed_dir = _make_dirs(tmp_path)
    code = "def run():\n    return 'disk'\n"
    (legacy_dir / "disk_helper.py").write_text(code, encoding="utf-8")

    db.upsert_helper(name="disk_helper", script_path="/app/helpers/disk_helper.py", code="")

    with caplog.at_level(logging.INFO, logger="brix.migrations"):
        _import_legacy_helper_code_v76(db, helper_dirs=(legacy_dir, managed_dir))

    helper = db.get_helper("disk_helper")
    assert helper is not None
    assert helper["code"] == code
    assert helper["content_hash"] == hashlib.sha256(code.encode("utf-8")).hexdigest()
    assert "imported helper 'disk_helper'" in caplog.text


def test_migration_skips_helpers_that_already_have_code(tmp_path, caplog) -> None:
    db = BrixDB(db_path=tmp_path / "hdb05-skip.db")
    legacy_dir, managed_dir = _make_dirs(tmp_path)
    existing_code = "def run():\n    return 'db'\n"
    disk_code = "def run():\n    return 'disk'\n"
    (managed_dir / "existing_helper.py").write_text(disk_code, encoding="utf-8")

    db.upsert_helper(
        name="existing_helper",
        script_path="/root/.brix/helpers/existing_helper.py",
        code=existing_code,
        content_hash="",
    )

    with caplog.at_level(logging.INFO, logger="brix.migrations"):
        _import_legacy_helper_code_v76(db, helper_dirs=(legacy_dir, managed_dir))

    helper = db.get_helper("existing_helper")
    assert helper is not None
    # Disk version wins when hashes differ
    assert helper["code"] == disk_code
    assert helper["content_hash"] == hashlib.sha256(disk_code.encode("utf-8")).hexdigest()
    assert "DB code differs from disk" in caplog.text


def test_migration_calculates_content_hash_for_helpers_with_code(tmp_path) -> None:
    db = BrixDB(db_path=tmp_path / "hdb05-hash.db")
    legacy_dir, managed_dir = _make_dirs(tmp_path)
    code = "def transform(value):\n    return value * 2\n"

    db.upsert_helper(
        name="hash_helper",
        script_path="db://hash_helper",
        code=code,
        content_hash="",
    )

    _import_legacy_helper_code_v76(db, helper_dirs=(legacy_dir, managed_dir))

    helper = db.get_helper("hash_helper")
    assert helper is not None
    assert helper["content_hash"] == hashlib.sha256(code.encode("utf-8")).hexdigest()
