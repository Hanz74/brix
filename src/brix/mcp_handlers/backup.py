"""Backup and Restore handler module — T-BRIX-DB-28.

Provides three MCP handlers:
- _handle_backup:      Create timestamped backups of brix.db + credentials.db
- _handle_restore:     Restore a database from a backup file
- _handle_backup_list: List available backups in a directory
"""
from __future__ import annotations

import shutil
import sqlite3
from datetime import datetime, timezone
from pathlib import Path


_DEFAULT_BACKUP_DIR = Path.home() / ".brix" / "backups"

# Databases to back up: (source_path, label_in_filename)
_BRIX_DB_PATH = Path.home() / ".brix" / "brix.db"
_CREDENTIALS_DB_PATH = Path.home() / ".brix" / "credentials.db"


def _is_valid_sqlite(path: Path) -> bool:
    """Return True if the file at *path* is a valid SQLite3 database."""
    try:
        conn = sqlite3.connect(str(path))
        conn.execute("PRAGMA integrity_check")
        conn.close()
        return True
    except Exception:
        return False


def _size_mb(path: Path) -> float:
    """Return file size in MB rounded to 3 decimal places."""
    try:
        return round(path.stat().st_size / (1024 * 1024), 3)
    except Exception:
        return 0.0


async def _handle_backup(arguments: dict) -> dict:
    """Create timestamped backups of brix.db and credentials.db.

    Parameters
    ----------
    path : str, optional
        Target directory for backup files.  Defaults to ~/.brix/backups.

    Returns
    -------
    dict with keys: success, path, files, size_mb, timestamp
    """
    backup_dir = Path(arguments.get("path", str(_DEFAULT_BACKUP_DIR)))

    try:
        backup_dir.mkdir(parents=True, exist_ok=True)
    except Exception as exc:
        return {"success": False, "error": f"Cannot create backup directory: {exc}"}

    # ISO timestamp for filename — colons replaced with dashes for FS compat
    ts = datetime.now(timezone.utc).strftime("%Y-%m-%dT%H-%M-%S")

    backed_up: list[dict] = []
    total_size = 0.0

    for src, label in [
        (_BRIX_DB_PATH, "brix"),
        (_CREDENTIALS_DB_PATH, "credentials"),
    ]:
        if not src.exists():
            # Skip silently — credentials.db may not exist on fresh installs
            continue
        dest_name = f"brix-backup-{label}-{ts}.db"
        dest = backup_dir / dest_name
        try:
            shutil.copy2(str(src), str(dest))
            size = _size_mb(dest)
            total_size += size
            backed_up.append({
                "source": str(src),
                "backup": str(dest),
                "size_mb": size,
            })
        except Exception as exc:
            return {"success": False, "error": f"Failed to copy {src.name}: {exc}"}

    if not backed_up:
        return {
            "success": False,
            "error": "No database files found to back up (neither brix.db nor credentials.db exists)",
        }

    return {
        "success": True,
        "path": str(backup_dir),
        "files": backed_up,
        "size_mb": round(total_size, 3),
        "timestamp": ts,
    }


async def _handle_restore(arguments: dict) -> dict:
    """Restore a database from a backup file.

    Parameters
    ----------
    backup_path : str
        Full path to the backup file to restore.
    target : str, optional
        Target database path.  If omitted the handler infers the target
        from the filename (brix.db or credentials.db).

    Returns
    -------
    dict with keys: success, restored_from, restored_to, size_mb
    """
    backup_path_str = arguments.get("backup_path", "").strip()
    if not backup_path_str:
        return {"success": False, "error": "Parameter 'backup_path' is required"}

    backup_path = Path(backup_path_str)
    if not backup_path.exists():
        return {"success": False, "error": f"Backup file not found: {backup_path}"}

    if not _is_valid_sqlite(backup_path):
        return {"success": False, "error": f"File is not a valid SQLite database: {backup_path}"}

    # Determine target from argument or infer from filename
    target_str = arguments.get("target", "").strip()
    if target_str:
        target = Path(target_str)
    else:
        name_lower = backup_path.name.lower()
        if "credentials" in name_lower:
            target = _CREDENTIALS_DB_PATH
        else:
            target = _BRIX_DB_PATH

    try:
        target.parent.mkdir(parents=True, exist_ok=True)
        shutil.copy2(str(backup_path), str(target))
    except Exception as exc:
        return {"success": False, "error": f"Failed to restore: {exc}"}

    return {
        "success": True,
        "restored_from": str(backup_path),
        "restored_to": str(target),
        "size_mb": _size_mb(target),
    }


async def _handle_backup_list(arguments: dict) -> dict:
    """List available backup files in a directory.

    Parameters
    ----------
    path : str, optional
        Directory to scan.  Defaults to ~/.brix/backups.

    Returns
    -------
    dict with keys: success, path, count, backups
    """
    backup_dir = Path(arguments.get("path", str(_DEFAULT_BACKUP_DIR)))

    if not backup_dir.exists():
        return {
            "success": True,
            "path": str(backup_dir),
            "count": 0,
            "backups": [],
        }

    try:
        entries: list[dict] = []
        for f in sorted(backup_dir.iterdir()):
            if not f.is_file() or f.suffix != ".db":
                continue
            try:
                stat = f.stat()
                created_at = datetime.fromtimestamp(
                    stat.st_mtime, tz=timezone.utc
                ).isoformat()
                size = round(stat.st_size / (1024 * 1024), 3)
            except Exception:
                created_at = "unknown"
                size = 0.0
            entries.append({
                "path": str(f),
                "name": f.name,
                "size_mb": size,
                "created_at": created_at,
            })

        return {
            "success": True,
            "path": str(backup_dir),
            "count": len(entries),
            "backups": entries,
        }
    except Exception as exc:
        return {"success": False, "error": str(exc)}
