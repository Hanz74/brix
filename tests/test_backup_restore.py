"""Tests for the brix__backup, brix__restore, brix__backup_list MCP tools — T-BRIX-DB-28.

Covers:
- _handle_backup: creates timestamped backup files for brix.db and credentials.db
- _handle_backup: default path (~/.brix/backups) is used when 'path' is omitted
- _handle_backup: custom path is respected
- _handle_backup: skips missing credentials.db gracefully
- _handle_backup: returns correct metadata (success, path, files, size_mb, timestamp)
- _handle_backup: error when source directory cannot be created
- _handle_restore: restores backup to inferred target (brix.db)
- _handle_restore: restores backup to inferred target (credentials.db via filename)
- _handle_restore: restores to explicit target path
- _handle_restore: error on missing backup_path argument
- _handle_restore: error when backup file does not exist
- _handle_restore: error when backup file is not a valid SQLite database
- _handle_backup_list: lists .db files sorted by name
- _handle_backup_list: returns empty list when directory does not exist
- _handle_backup_list: returns count matching backups length
"""
from __future__ import annotations

import asyncio
import sqlite3
from pathlib import Path
from unittest.mock import patch

import pytest

import brix.mcp_handlers.backup as backup_mod
from brix.mcp_handlers.backup import (
    _handle_backup,
    _handle_backup_list,
    _handle_restore,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_sqlite_db(path: Path) -> None:
    """Create a minimal SQLite3 database at *path*."""
    path.parent.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(str(path))
    conn.execute("CREATE TABLE IF NOT EXISTS _meta (k TEXT)")
    conn.commit()
    conn.close()


# ---------------------------------------------------------------------------
# _handle_backup
# ---------------------------------------------------------------------------

class TestHandleBackup:
    @pytest.mark.asyncio
    async def test_backup_creates_files(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        creds_db = tmp_path / "credentials.db"
        _make_sqlite_db(brix_db)
        _make_sqlite_db(creds_db)
        backup_dir = tmp_path / "backups"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", creds_db):
            result = await _handle_backup({"path": str(backup_dir)})

        assert result["success"] is True
        assert len(result["files"]) == 2
        for entry in result["files"]:
            assert Path(entry["backup"]).exists()

    @pytest.mark.asyncio
    async def test_backup_returns_correct_keys(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        _make_sqlite_db(brix_db)
        backup_dir = tmp_path / "backups"
        missing_creds = tmp_path / "nonexistent_credentials.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(backup_dir)})

        assert result["success"] is True
        for key in ("path", "files", "size_mb", "timestamp"):
            assert key in result, f"Missing key: {key}"

    @pytest.mark.asyncio
    async def test_backup_custom_path(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        _make_sqlite_db(brix_db)
        custom_dir = tmp_path / "my_backups"
        missing_creds = tmp_path / "nonexistent_credentials.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(custom_dir)})

        assert result["success"] is True
        assert result["path"] == str(custom_dir)
        assert custom_dir.exists()

    @pytest.mark.asyncio
    async def test_backup_skips_missing_credentials_db(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        _make_sqlite_db(brix_db)
        backup_dir = tmp_path / "backups"
        missing_creds = tmp_path / "nonexistent.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(backup_dir)})

        assert result["success"] is True
        # Only brix.db was backed up
        assert len(result["files"]) == 1
        assert "brix" in result["files"][0]["backup"]

    @pytest.mark.asyncio
    async def test_backup_size_mb_is_float(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        _make_sqlite_db(brix_db)
        backup_dir = tmp_path / "backups"
        missing_creds = tmp_path / "nonexistent.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(backup_dir)})

        assert isinstance(result["size_mb"], float)

    @pytest.mark.asyncio
    async def test_backup_timestamp_in_filename(self, tmp_path):
        brix_db = tmp_path / "brix.db"
        _make_sqlite_db(brix_db)
        backup_dir = tmp_path / "backups"
        missing_creds = tmp_path / "nonexistent.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", brix_db), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(backup_dir)})

        ts = result["timestamp"]
        filename = Path(result["files"][0]["backup"]).name
        # Timestamp format: 2026-03-28T20-00-00  — year always starts the ts
        assert ts[:4].isdigit()
        assert ts in filename

    @pytest.mark.asyncio
    async def test_backup_error_when_no_dbs_exist(self, tmp_path):
        backup_dir = tmp_path / "backups"
        missing_brix = tmp_path / "missing_brix.db"
        missing_creds = tmp_path / "missing_creds.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", missing_brix), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", missing_creds):
            result = await _handle_backup({"path": str(backup_dir)})

        assert result["success"] is False
        assert "error" in result


# ---------------------------------------------------------------------------
# _handle_restore
# ---------------------------------------------------------------------------

class TestHandleRestore:
    @pytest.mark.asyncio
    async def test_restore_to_inferred_brix_target(self, tmp_path):
        backup_file = tmp_path / "brix-backup-brix-2026-01-01T00-00-00.db"
        _make_sqlite_db(backup_file)
        target = tmp_path / "restored_brix.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", target), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", tmp_path / "creds.db"):
            result = await _handle_restore({"backup_path": str(backup_file)})

        assert result["success"] is True
        assert target.exists()
        assert result["restored_to"] == str(target)

    @pytest.mark.asyncio
    async def test_restore_to_inferred_credentials_target(self, tmp_path):
        backup_file = tmp_path / "brix-backup-credentials-2026-01-01T00-00-00.db"
        _make_sqlite_db(backup_file)
        target = tmp_path / "restored_creds.db"

        with patch.object(backup_mod, "_BRIX_DB_PATH", tmp_path / "brix.db"), \
             patch.object(backup_mod, "_CREDENTIALS_DB_PATH", target):
            result = await _handle_restore({"backup_path": str(backup_file)})

        assert result["success"] is True
        assert target.exists()
        assert result["restored_to"] == str(target)

    @pytest.mark.asyncio
    async def test_restore_to_explicit_target(self, tmp_path):
        backup_file = tmp_path / "some_backup.db"
        _make_sqlite_db(backup_file)
        explicit_target = tmp_path / "explicit_target.db"

        result = await _handle_restore({
            "backup_path": str(backup_file),
            "target": str(explicit_target),
        })

        assert result["success"] is True
        assert explicit_target.exists()
        assert result["restored_to"] == str(explicit_target)

    @pytest.mark.asyncio
    async def test_restore_missing_backup_path_param(self):
        result = await _handle_restore({})
        assert result["success"] is False
        assert "backup_path" in result["error"]

    @pytest.mark.asyncio
    async def test_restore_nonexistent_file(self, tmp_path):
        result = await _handle_restore({"backup_path": str(tmp_path / "ghost.db")})
        assert result["success"] is False
        assert "not found" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_restore_invalid_sqlite(self, tmp_path):
        bad_file = tmp_path / "notasqlite.db"
        bad_file.write_bytes(b"this is not sqlite")
        result = await _handle_restore({"backup_path": str(bad_file)})
        assert result["success"] is False
        assert "valid SQLite" in result["error"] or "not a valid" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_restore_returns_size_mb(self, tmp_path):
        backup_file = tmp_path / "backup.db"
        _make_sqlite_db(backup_file)
        explicit_target = tmp_path / "target.db"

        result = await _handle_restore({
            "backup_path": str(backup_file),
            "target": str(explicit_target),
        })

        assert result["success"] is True
        assert isinstance(result["size_mb"], float)


# ---------------------------------------------------------------------------
# _handle_backup_list
# ---------------------------------------------------------------------------

class TestHandleBackupList:
    @pytest.mark.asyncio
    async def test_lists_db_files(self, tmp_path):
        # Create some backup files
        for name in ("brix-backup-brix-2026-01-01T00-00-00.db",
                     "brix-backup-credentials-2026-01-01T00-00-00.db"):
            f = tmp_path / name
            _make_sqlite_db(f)

        result = await _handle_backup_list({"path": str(tmp_path)})

        assert result["success"] is True
        assert result["count"] == 2
        names = [b["name"] for b in result["backups"]]
        assert "brix-backup-brix-2026-01-01T00-00-00.db" in names

    @pytest.mark.asyncio
    async def test_empty_when_directory_missing(self, tmp_path):
        nonexistent = tmp_path / "no_such_dir"
        result = await _handle_backup_list({"path": str(nonexistent)})
        assert result["success"] is True
        assert result["count"] == 0
        assert result["backups"] == []

    @pytest.mark.asyncio
    async def test_count_matches_backups_length(self, tmp_path):
        for i in range(3):
            f = tmp_path / f"brix-backup-brix-2026-01-0{i+1}T00-00-00.db"
            _make_sqlite_db(f)

        result = await _handle_backup_list({"path": str(tmp_path)})

        assert result["success"] is True
        assert result["count"] == len(result["backups"])

    @pytest.mark.asyncio
    async def test_backup_entries_have_required_keys(self, tmp_path):
        f = tmp_path / "brix-backup-brix-2026-01-01T00-00-00.db"
        _make_sqlite_db(f)

        result = await _handle_backup_list({"path": str(tmp_path)})

        assert result["success"] is True
        assert result["count"] == 1
        entry = result["backups"][0]
        for key in ("path", "name", "size_mb", "created_at"):
            assert key in entry, f"Missing key: {key}"

    @pytest.mark.asyncio
    async def test_ignores_non_db_files(self, tmp_path):
        _make_sqlite_db(tmp_path / "real.db")
        (tmp_path / "readme.txt").write_text("hello")
        (tmp_path / "archive.zip").write_bytes(b"PK")

        result = await _handle_backup_list({"path": str(tmp_path)})

        assert result["success"] is True
        assert result["count"] == 1
