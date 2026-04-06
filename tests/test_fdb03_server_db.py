"""T-BRIX-FDB-03: MCP servers are managed from DB, not YAML."""

from pathlib import Path

from brix.db import BrixDB
from brix.runners.mcp import load_server_config
from brix.server_manager import ServerManager


def test_server_manager_crud_uses_db(tmp_path: Path):
    mgr = ServerManager(servers_path=tmp_path / "servers.yaml")
    server_name = "fdb03-manager-server"

    created = mgr.add(
        server_name,
        "node",
        ["/app/index.js"],
        {"TOKEN": "abc123"},
    )
    assert created == {
        "name": server_name,
        "command": "node",
        "args": ["/app/index.js"],
        "env": {"TOKEN": "abc123"},
        "tools_prefix": None,
        "transport": "stdio",
        "url": "",
        "unwrap_json": False,
    }
    assert not (tmp_path / "servers.yaml").exists()

    fetched = mgr.get(server_name)
    assert fetched == created

    listed = mgr.list_all()
    assert created in listed

    db = BrixDB(tmp_path / "brix.db")
    row = db.get_mcp_server(server_name)
    assert row is not None
    assert row["name"] == server_name
    assert row["command"] == "node"
    assert row["args"] == ["/app/index.js"]
    assert row["env"] == {"TOKEN": "abc123"}

    assert mgr.remove(server_name) is True
    assert mgr.get(server_name) is None
    assert db.get_mcp_server(server_name) is None


def test_runner_load_server_config_returns_db_entry(tmp_path: Path):
    db = BrixDB(tmp_path / "brix.db")
    server_name = "fdb03-runner-server"
    db.upsert_mcp_server(
        name=server_name,
        command="python3",
        args=["-m", "server"],
        env={"API_KEY": "secret"},
        transport="stdio",
        unwrap_json=True,
    )

    config = load_server_config(server_name, tmp_path / "servers.yaml")

    assert config.name == server_name
    assert config.command == "python3"
    assert config.args == ["-m", "server"]
    assert config.env == {"API_KEY": "secret"}
    assert config.transport == "stdio"
    assert config.unwrap_json is True
