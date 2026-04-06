"""Tests for T-BRIX-FDB-01 and T-BRIX-FDB-02 schema changes."""

from pathlib import Path

import pytest
import yaml

from brix.db import BrixDB


@pytest.fixture
def db(tmp_path, monkeypatch):
    """Isolated temp DB with isolated ~/.brix for migration imports."""
    monkeypatch.setenv("HOME", str(tmp_path))
    (tmp_path / ".brix").mkdir(parents=True, exist_ok=True)
    return BrixDB(db_path=tmp_path / "brix.db")


def test_mcp_server_crud_roundtrip(db: BrixDB):
    stored = db.upsert_mcp_server(
        name="demo-server",
        command="python",
        args=["-m", "demo.server"],
        env={"API_KEY": "secret"},
        tools_prefix="demo__",
        transport="stdio",
        url="",
        unwrap_json=True,
        description="Demo MCP server",
    )

    assert stored["name"] == "demo-server"
    assert stored["command"] == "python"
    assert stored["args"] == ["-m", "demo.server"]
    assert stored["env"] == {"API_KEY": "secret"}
    assert stored["tools_prefix"] == "demo__"
    assert stored["transport"] == "stdio"
    assert stored["unwrap_json"] is True
    assert stored["description"] == "Demo MCP server"

    fetched = db.get_mcp_server("demo-server")
    assert fetched == stored

    rows = db.list_mcp_servers()
    assert [row["name"] for row in rows] == ["demo-server"]

    assert db.delete_mcp_server("demo-server") is True
    assert db.get_mcp_server("demo-server") is None
    assert db.delete_mcp_server("demo-server") is False


def test_env_profile_crud_roundtrip(db: BrixDB):
    stored = db.upsert_env_profile(
        name="dev",
        env={"BASE_URL": "http://localhost:8000"},
        input_defaults={"limit": 10, "dry_run": True},
        is_default=False,
        description="Development profile",
    )

    assert stored["name"] == "dev"
    assert stored["env"] == {"BASE_URL": "http://localhost:8000"}
    assert stored["input_defaults"] == {"limit": 10, "dry_run": True}
    assert stored["is_default"] is False
    assert stored["description"] == "Development profile"

    fetched = db.get_env_profile("dev")
    assert fetched == stored

    rows = db.list_env_profiles()
    assert [row["name"] for row in rows] == ["dev"]

    assert db.delete_env_profile("dev") is True
    assert db.get_env_profile("dev") is None
    assert db.delete_env_profile("dev") is False


def test_get_default_env_profile_returns_default_row(db: BrixDB):
    db.upsert_env_profile(
        name="dev",
        env={"MODE": "dev"},
        input_defaults={"limit": 5},
        is_default=False,
    )
    db.upsert_env_profile(
        name="prod",
        env={"MODE": "prod"},
        input_defaults={"limit": 100},
        is_default=True,
    )

    default_profile = db.get_default_env_profile()
    assert default_profile is not None
    assert default_profile["name"] == "prod"
    assert default_profile["is_default"] is True

    dev = db.get_env_profile("dev")
    prod = db.get_env_profile("prod")
    assert dev is not None and dev["is_default"] is False
    assert prod is not None and prod["is_default"] is True


def test_migration_v78_imports_servers_yaml(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    brix_dir = tmp_path / ".brix"
    brix_dir.mkdir(parents=True, exist_ok=True)
    (brix_dir / "servers.yaml").write_text(
        yaml.safe_dump(
            {
                "servers": {
                    "demo-server": {
                        "command": "uvx",
                        "args": ["demo-mcp"],
                        "env": {"TOKEN": "abc"},
                        "tools_prefix": "demo__",
                        "transport": "stdio",
                        "url": "",
                        "unwrap_json": True,
                        "description": "Imported server",
                    }
                }
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    db = BrixDB(db_path=tmp_path / "brix.db")
    row = db.get_mcp_server("demo-server")

    assert row is not None
    assert row["command"] == "uvx"
    assert row["args"] == ["demo-mcp"]
    assert row["env"] == {"TOKEN": "abc"}
    assert row["tools_prefix"] == "demo__"
    assert row["unwrap_json"] is True


def test_migration_v79_imports_profiles_yaml(tmp_path, monkeypatch):
    monkeypatch.setenv("HOME", str(tmp_path))
    brix_dir = tmp_path / ".brix"
    brix_dir.mkdir(parents=True, exist_ok=True)
    (brix_dir / "profiles.yaml").write_text(
        yaml.safe_dump(
            {
                "default_profile": "prod",
                "profiles": {
                    "dev": {
                        "env": {"MODE": "dev"},
                        "input_defaults": {"limit": 10},
                        "description": "Development",
                    },
                    "prod": {
                        "env": {"MODE": "prod"},
                        "input_defaults": {"limit": 100},
                        "description": "Production",
                    },
                },
            },
            sort_keys=False,
        ),
        encoding="utf-8",
    )

    db = BrixDB(db_path=tmp_path / "brix.db")

    dev = db.get_env_profile("dev")
    prod = db.get_env_profile("prod")
    default_profile = db.get_default_env_profile()

    assert dev is not None
    assert prod is not None
    assert dev["is_default"] is False
    assert prod["is_default"] is True
    assert prod["description"] == "Production"
    assert default_profile is not None
    assert default_profile["name"] == "prod"
