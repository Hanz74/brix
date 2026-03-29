"""Tests for T-BRIX-CRUD-01 — fehlende get/update/search Handler.

Covers (2 tests each = 22 tests total):
1.  _handle_get_connection         — happy path + not found
2.  _handle_update_connection      — happy path + not found
3.  _handle_search_connections     — results + empty
4.  _handle_trigger_group_get      — happy path + not found
5.  _handle_trigger_group_update   — happy path + not found
6.  _handle_search_trigger_groups  — results + empty
7.  _handle_search_triggers        — results + empty
8.  _handle_search_variables       — results + empty
9.  _handle_search_profiles        — results + empty
10. _handle_get_alert_rule         — happy path + not found
11. _handle_search_alert_rules     — results + empty
"""
from __future__ import annotations

import pytest

from helpers import run_coro as run


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

@pytest.fixture
def tmp_db(tmp_path):
    """BrixDB backed by a temp file."""
    from brix.db import BrixDB
    return BrixDB(db_path=tmp_path / "brix.db")


@pytest.fixture
def conn_mgr(tmp_db):
    """ConnectionManager with one pre-registered SQLite connection."""
    from brix.connections import ConnectionManager
    mgr = ConnectionManager(tmp_db)
    mgr.register("test-conn", "test.db", driver="sqlite",
                  description="A test connection", project="test-proj")
    return mgr, tmp_db


@pytest.fixture
def tg_store(tmp_path):
    """TriggerGroupStore with one pre-created group."""
    from brix.triggers.store import TriggerGroupStore
    from brix.db import BrixDB
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = TriggerGroupStore(db=db)
    store.add("email-group", triggers=[], description="handles email triggers")
    return store


@pytest.fixture
def tr_store(tmp_path):
    """TriggerStore with one pre-created trigger."""
    from brix.triggers.store import TriggerStore
    from brix.db import BrixDB
    db = BrixDB(db_path=tmp_path / "brix.db")
    store = TriggerStore(db=db)
    store.add(name="mail-watcher", type="mail",
              pipeline="process-mail", config={}, enabled=True)
    return store


@pytest.fixture
def var_db(tmp_db):
    """BrixDB with one variable."""
    tmp_db.variable_set("my_var", "hello", description="test variable", project="proj-x")
    return tmp_db


@pytest.fixture
def profile_db(tmp_db):
    """BrixDB with one profile."""
    tmp_db.profile_set("my-profile", {"timeout": 30}, description="default profile")
    return tmp_db


@pytest.fixture
def alert_db(tmp_db):
    """BrixDB with one alert rule."""
    tmp_db.alert_rule_add(name="high-failure", condition="pipeline_failed",
                          channel="log", project="buddy")
    return tmp_db


# ===========================================================================
# 1. get_connection
# ===========================================================================

class TestGetConnection:
    def test_happy_path(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_get_connection
        from brix.connections import ConnectionManager
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_get_connection({"name": "test-conn"}))
        assert result["success"] is True
        assert result["name"] == "test-conn"
        assert result["driver"] == "sqlite"
        assert "note" in result

    def test_not_found(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_get_connection
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_get_connection({"name": "no-such-conn"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 2. update_connection
# ===========================================================================

class TestUpdateConnection:
    def test_happy_path(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_update_connection
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_update_connection({
                "name": "test-conn",
                "description": "updated description",
                "project": "new-project",
            }))
        assert result["success"] is True
        assert result["description"] == "updated description"
        assert result["project"] == "new-project"

    def test_not_found(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_update_connection
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_update_connection({"name": "ghost-conn", "description": "x"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 3. search_connections
# ===========================================================================

class TestSearchConnections:
    def test_matches(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_search_connections
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_search_connections({"query": "test"}))
        assert result["success"] is True
        assert result["total"] >= 1
        assert any("test" in c["name"].lower() for c in result["connections"])

    def test_no_matches(self, conn_mgr):
        from brix.mcp_handlers.connections import _handle_search_connections
        import unittest.mock as mock
        mgr, db = conn_mgr
        with mock.patch("brix.db.BrixDB", return_value=db), \
             mock.patch("brix.connections.ConnectionManager", return_value=mgr):
            result = run(_handle_search_connections({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0


# ===========================================================================
# 4. get_trigger_group
# ===========================================================================

class TestGetTriggerGroup:
    def test_happy_path(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_trigger_group_get
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_trigger_group_get({"name": "email-group"}))
        assert result["success"] is True
        assert result["group"]["name"] == "email-group"

    def test_not_found(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_trigger_group_get
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_trigger_group_get({"name": "does-not-exist"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 5. update_trigger_group
# ===========================================================================

class TestUpdateTriggerGroup:
    def test_happy_path(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_trigger_group_update
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_trigger_group_update({
                "name": "email-group",
                "description": "updated desc",
                "project": "my-project",
            }))
        assert result["success"] is True
        assert result["group"]["description"] == "updated desc"

    def test_not_found(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_trigger_group_update
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_trigger_group_update({"name": "ghost-grp", "description": "x"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 6. search_trigger_groups
# ===========================================================================

class TestSearchTriggerGroups:
    def test_matches(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_search_trigger_groups
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_search_trigger_groups({"query": "email"}))
        assert result["success"] is True
        assert result["total"] >= 1

    def test_no_matches(self, tg_store):
        from brix.mcp_handlers.triggers import _handle_search_trigger_groups
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerGroupStore", return_value=tg_store):
            result = run(_handle_search_trigger_groups({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0


# ===========================================================================
# 7. search_triggers
# ===========================================================================

class TestSearchTriggers:
    def test_matches(self, tr_store):
        from brix.mcp_handlers.triggers import _handle_search_triggers
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerStore", return_value=tr_store):
            result = run(_handle_search_triggers({"query": "mail"}))
        assert result["success"] is True
        assert result["total"] >= 1

    def test_no_matches(self, tr_store):
        from brix.mcp_handlers.triggers import _handle_search_triggers
        import unittest.mock as mock
        with mock.patch("brix.triggers.store.TriggerStore", return_value=tr_store):
            result = run(_handle_search_triggers({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0


# ===========================================================================
# 8. search_variables
# ===========================================================================

class TestSearchVariables:
    def test_matches(self, var_db):
        from brix.mcp_handlers.variables import _handle_search_variables
        import unittest.mock as mock
        # BrixDB is imported at module level in variables.py — patch there
        with mock.patch("brix.mcp_handlers.variables.BrixDB", return_value=var_db):
            result = run(_handle_search_variables({"query": "my_var"}))
        assert result["success"] is True
        assert result["total"] >= 1
        assert any(v["name"] == "my_var" for v in result["variables"])

    def test_no_matches(self, var_db):
        from brix.mcp_handlers.variables import _handle_search_variables
        import unittest.mock as mock
        with mock.patch("brix.mcp_handlers.variables.BrixDB", return_value=var_db):
            result = run(_handle_search_variables({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0


# ===========================================================================
# 9. search_profiles
# ===========================================================================

class TestSearchProfiles:
    def test_matches(self, profile_db):
        from brix.mcp_handlers.profiles import _handle_search_profiles
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=profile_db):
            result = run(_handle_search_profiles({"query": "my-profile"}))
        assert result["success"] is True
        assert result["total"] >= 1
        assert any(p["name"] == "my-profile" for p in result["profiles"])

    def test_no_matches(self, profile_db):
        from brix.mcp_handlers.profiles import _handle_search_profiles
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=profile_db):
            result = run(_handle_search_profiles({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0


# ===========================================================================
# 10. get_alert_rule
# ===========================================================================

class TestGetAlertRule:
    def test_happy_path(self, alert_db):
        from brix.mcp_handlers.alerts import _handle_get_alert_rule
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=alert_db):
            result = run(_handle_get_alert_rule({"name": "high-failure"}))
        assert result["success"] is True
        assert result["rule"]["name"] == "high-failure"
        assert result["rule"]["condition"] == "pipeline_failed"

    def test_not_found(self, alert_db):
        from brix.mcp_handlers.alerts import _handle_get_alert_rule
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=alert_db):
            result = run(_handle_get_alert_rule({"name": "no-such-rule"}))
        assert result["success"] is False
        assert "not found" in result["error"].lower()


# ===========================================================================
# 11. search_alert_rules
# ===========================================================================

class TestSearchAlertRules:
    def test_matches(self, alert_db):
        from brix.mcp_handlers.alerts import _handle_search_alert_rules
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=alert_db):
            result = run(_handle_search_alert_rules({"query": "failure"}))
        assert result["success"] is True
        assert result["total"] >= 1

    def test_no_matches(self, alert_db):
        from brix.mcp_handlers.alerts import _handle_search_alert_rules
        import unittest.mock as mock
        with mock.patch("brix.db.BrixDB", return_value=alert_db):
            result = run(_handle_search_alert_rules({"query": "zzznomatch999"}))
        assert result["success"] is True
        assert result["total"] == 0
