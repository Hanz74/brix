"""Tests for brix.profiles — Environment Profile management (T-BRIX-V4-18)."""

import os
from pathlib import Path

import pytest
import yaml

from brix.profiles import (
    ProfileManager,
    ProfileNotFoundError,
    _resolve_env_values,
    BRIX_PROFILE_ENV,
)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_profiles_yaml(tmp_path: Path, data: dict) -> Path:
    """Write a profiles.yaml to tmp_path and return its path."""
    p = tmp_path / "profiles.yaml"
    p.write_text(yaml.dump(data))
    return p


# ---------------------------------------------------------------------------
# ProfileManager — loading / listing
# ---------------------------------------------------------------------------


def test_profile_manager_empty_file(tmp_path):
    """ProfileManager returns empty list when profiles.yaml does not exist."""
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.list_profiles() == []


def test_profile_manager_list_profiles(tmp_path):
    """list_profiles returns the names defined under 'profiles' key."""
    _make_profiles_yaml(tmp_path, {
        "profiles": {
            "dev": {"env": {}, "input_defaults": {}},
            "prod": {"env": {}, "input_defaults": {}},
        }
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    names = mgr.list_profiles()
    assert "dev" in names
    assert "prod" in names


def test_profile_manager_get_default_profile(tmp_path):
    """get_default_profile returns the configured default."""
    _make_profiles_yaml(tmp_path, {
        "default_profile": "dev",
        "profiles": {"dev": {}, "prod": {}},
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.get_default_profile() == "dev"


def test_profile_manager_no_default(tmp_path):
    """get_default_profile returns None when not configured."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.get_default_profile() is None


# ---------------------------------------------------------------------------
# load_profile
# ---------------------------------------------------------------------------


def test_load_profile_returns_env_and_input_defaults(tmp_path):
    """load_profile returns env dict and input_defaults."""
    _make_profiles_yaml(tmp_path, {
        "profiles": {
            "dev": {
                "env": {"MY_KEY": "dev-value"},
                "input_defaults": {"limit": 10},
            }
        }
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    config = mgr.load_profile("dev")
    assert config["env"]["MY_KEY"] == "dev-value"
    assert config["input_defaults"]["limit"] == 10


def test_load_profile_not_found(tmp_path):
    """load_profile raises ProfileNotFoundError for unknown profiles."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    with pytest.raises(ProfileNotFoundError, match="ghost"):
        mgr.load_profile("ghost")


def test_load_profile_empty_sections(tmp_path):
    """load_profile handles profiles with no env or input_defaults."""
    _make_profiles_yaml(tmp_path, {"profiles": {"minimal": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    config = mgr.load_profile("minimal")
    assert config["env"] == {}
    assert config["input_defaults"] == {}


# ---------------------------------------------------------------------------
# active_profile_name — priority resolution
# ---------------------------------------------------------------------------


def test_active_profile_override_takes_highest_priority(tmp_path, monkeypatch):
    """override argument beats BRIX_PROFILE env var and default_profile."""
    monkeypatch.setenv(BRIX_PROFILE_ENV, "staging")
    _make_profiles_yaml(tmp_path, {
        "default_profile": "dev",
        "profiles": {"dev": {}, "staging": {}, "prod": {}},
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.active_profile_name(override="prod") == "prod"


def test_active_profile_env_var_beats_default(tmp_path, monkeypatch):
    """BRIX_PROFILE env var beats default_profile."""
    monkeypatch.setenv(BRIX_PROFILE_ENV, "staging")
    _make_profiles_yaml(tmp_path, {
        "default_profile": "dev",
        "profiles": {"dev": {}, "staging": {}},
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.active_profile_name() == "staging"


def test_active_profile_default_profile_fallback(tmp_path, monkeypatch):
    """Falls back to default_profile when no override and no env var."""
    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)
    _make_profiles_yaml(tmp_path, {
        "default_profile": "dev",
        "profiles": {"dev": {}},
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.active_profile_name() == "dev"


def test_active_profile_none_when_nothing_configured(tmp_path, monkeypatch):
    """Returns None when no profile is configured anywhere."""
    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    assert mgr.active_profile_name() is None


# ---------------------------------------------------------------------------
# apply_profile — env var injection
# ---------------------------------------------------------------------------


def test_apply_profile_injects_env_vars(tmp_path, monkeypatch):
    """apply_profile sets env vars from the profile into os.environ."""
    monkeypatch.delenv("MY_TEST_KEY", raising=False)
    _make_profiles_yaml(tmp_path, {
        "profiles": {
            "dev": {"env": {"MY_TEST_KEY": "injected-value"}}
        }
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.apply_profile("dev")
    assert os.environ.get("MY_TEST_KEY") == "injected-value"


def test_apply_profile_none_does_nothing(tmp_path, monkeypatch):
    """apply_profile(None) returns empty config without side effects."""
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    config = mgr.apply_profile(None)
    assert config == {"env": {}, "input_defaults": {}}


def test_apply_profile_returns_config(tmp_path):
    """apply_profile returns the profile config dict."""
    _make_profiles_yaml(tmp_path, {
        "profiles": {
            "dev": {
                "env": {"KEY": "val"},
                "input_defaults": {"limit": 5},
            }
        }
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    config = mgr.apply_profile("dev")
    assert config["env"]["KEY"] == "val"
    assert config["input_defaults"]["limit"] == 5


# ---------------------------------------------------------------------------
# save_profile / delete_profile / set_default
# ---------------------------------------------------------------------------


def test_save_profile_creates_file(tmp_path):
    """save_profile creates profiles.yaml if it does not exist."""
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.save_profile("test", env={"FOO": "bar"}, input_defaults={"x": 1})
    assert (tmp_path / "profiles.yaml").exists()
    data = yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert data["profiles"]["test"]["env"]["FOO"] == "bar"
    assert data["profiles"]["test"]["input_defaults"]["x"] == 1


def test_save_profile_merges_existing(tmp_path):
    """save_profile does not overwrite other profiles."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {"env": {"A": "1"}}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.save_profile("prod", env={"B": "2"})
    data = yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert "dev" in data["profiles"]
    assert "prod" in data["profiles"]


def test_delete_profile(tmp_path):
    """delete_profile removes the profile from the file."""
    _make_profiles_yaml(tmp_path, {
        "profiles": {"dev": {}, "prod": {}}
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.delete_profile("dev")
    assert "dev" not in mgr.list_profiles()
    assert "prod" in mgr.list_profiles()


def test_delete_profile_not_found(tmp_path):
    """delete_profile raises ProfileNotFoundError for unknown profiles."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    with pytest.raises(ProfileNotFoundError):
        mgr.delete_profile("ghost")


def test_set_default(tmp_path):
    """set_default writes default_profile to the file."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {}, "prod": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.set_default("prod")
    data = yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert data["default_profile"] == "prod"


def test_set_default_clear(tmp_path):
    """set_default(None) removes default_profile."""
    _make_profiles_yaml(tmp_path, {
        "default_profile": "dev",
        "profiles": {"dev": {}}
    })
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    mgr.set_default(None)
    data = yaml.safe_load((tmp_path / "profiles.yaml").read_text())
    assert "default_profile" not in data


def test_set_default_invalid_profile(tmp_path):
    """set_default raises ProfileNotFoundError for non-existent profile."""
    _make_profiles_yaml(tmp_path, {"profiles": {"dev": {}}})
    mgr = ProfileManager(tmp_path / "profiles.yaml")
    with pytest.raises(ProfileNotFoundError):
        mgr.set_default("ghost")


# ---------------------------------------------------------------------------
# _resolve_env_values — ${VAR} substitution
# ---------------------------------------------------------------------------


def test_resolve_env_values_static():
    """Static values are returned unchanged."""
    result = _resolve_env_values({"KEY": "static-value"})
    assert result["KEY"] == "static-value"


def test_resolve_env_values_substitution(monkeypatch):
    """${VAR} is replaced with the OS env var value."""
    monkeypatch.setenv("MY_SECRET", "resolved-secret")
    result = _resolve_env_values({"TOKEN": "${MY_SECRET}"})
    assert result["TOKEN"] == "resolved-secret"


def test_resolve_env_values_missing_env(monkeypatch):
    """${VAR} resolves to empty string when the env var is not set."""
    monkeypatch.delenv("MISSING_VAR", raising=False)
    result = _resolve_env_values({"TOKEN": "${MISSING_VAR}"})
    assert result["TOKEN"] == ""


def test_resolve_env_values_partial_string_not_substituted():
    """Strings with ${} in the middle (not full-match) are kept as-is."""
    result = _resolve_env_values({"URL": "https://api.${DOMAIN}/v1"})
    # Not a full ${VAR} match — returned as literal string
    assert result["URL"] == "https://api.${DOMAIN}/v1"


# ---------------------------------------------------------------------------
# Integration with PipelineContext
# ---------------------------------------------------------------------------


def test_context_from_pipeline_with_profile_injects_env(tmp_path, monkeypatch):
    """from_pipeline with a profile injects env vars before credential resolution."""
    import yaml as _yaml
    from brix.context import PipelineContext
    from brix.loader import PipelineLoader

    # Profile sets PROFILE_TEST_KEY
    profiles_path = tmp_path / "profiles.yaml"
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "dev": {
                "env": {"PROFILE_TEST_KEY": "profile-injected"},
                "input_defaults": {},
            }
        }
    }))

    pipeline_yaml = """
name: profile-test
credentials:
  api_key:
    env: PROFILE_TEST_KEY
steps:
  - id: step1
    type: cli
    args: ["echo", "hi"]
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)

    # Ensure the env var is NOT pre-set in the environment
    monkeypatch.delenv("PROFILE_TEST_KEY", raising=False)

    # Monkeypatch ProfileManager to use our tmp profiles file
    from brix import profiles as _profiles_module
    original_path = _profiles_module.PROFILES_PATH
    _profiles_module.PROFILES_PATH = profiles_path

    try:
        from brix.profiles import ProfileManager
        # Also patch the class default so from_pipeline uses our path
        original_init = ProfileManager.__init__

        def patched_init(self, path=None):
            original_init(self, path or profiles_path)

        monkeypatch.setattr(ProfileManager, "__init__", patched_init)

        ctx = PipelineContext.from_pipeline(pipeline, profile="dev")
        assert ctx.credentials["api_key"] == "profile-injected"
        assert ctx._active_profile == "dev"
    finally:
        _profiles_module.PROFILES_PATH = original_path


def test_context_from_pipeline_profile_input_defaults(tmp_path, monkeypatch):
    """Profile input_defaults fill pipeline params without user_input or pipeline default."""
    import yaml as _yaml
    from brix.context import PipelineContext
    from brix.loader import PipelineLoader

    profiles_path = tmp_path / "profiles.yaml"
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "prod": {
                "env": {},
                "input_defaults": {"limit": 500},
            }
        }
    }))

    # Pipeline has a param 'limit' with NO default
    pipeline_yaml = """
name: input-defaults-test
input:
  limit:
    type: integer
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)

    from brix.profiles import ProfileManager
    original_init = ProfileManager.__init__

    def patched_init(self, path=None):
        original_init(self, path or profiles_path)

    monkeypatch.setattr(ProfileManager, "__init__", patched_init)

    ctx = PipelineContext.from_pipeline(pipeline, profile="prod")
    assert ctx.input.get("limit") == 500


def test_context_from_pipeline_user_input_beats_profile_default(tmp_path, monkeypatch):
    """User input takes priority over profile input_defaults."""
    import yaml as _yaml
    from brix.context import PipelineContext
    from brix.loader import PipelineLoader

    profiles_path = tmp_path / "profiles.yaml"
    profiles_path.write_text(_yaml.dump({
        "profiles": {
            "prod": {
                "env": {},
                "input_defaults": {"limit": 500},
            }
        }
    }))

    pipeline_yaml = """
name: priority-test
input:
  limit:
    type: integer
    default: 10
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)

    from brix.profiles import ProfileManager
    original_init = ProfileManager.__init__

    def patched_init(self, path=None):
        original_init(self, path or profiles_path)

    monkeypatch.setattr(ProfileManager, "__init__", patched_init)

    ctx = PipelineContext.from_pipeline(pipeline, user_input={"limit": 99}, profile="prod")
    # user_input (99) > profile default (500) > pipeline default (10)
    assert ctx.input["limit"] == 99


def test_context_from_pipeline_no_profile(tmp_path, monkeypatch):
    """from_pipeline with no profile works as before (backwards compatible)."""
    from brix.context import PipelineContext
    from brix.loader import PipelineLoader

    monkeypatch.delenv(BRIX_PROFILE_ENV, raising=False)

    from brix.profiles import ProfileManager
    original_init = ProfileManager.__init__

    def patched_init(self, path=None):
        original_init(self, path or tmp_path / "profiles.yaml")  # non-existent file

    monkeypatch.setattr(ProfileManager, "__init__", patched_init)

    pipeline_yaml = """
name: no-profile-test
steps:
  - id: s1
    type: cli
    args: ["echo", "ok"]
"""
    loader = PipelineLoader()
    pipeline = loader.load_from_string(pipeline_yaml)
    ctx = PipelineContext.from_pipeline(pipeline)
    # Should work fine; _active_profile is None
    assert ctx._active_profile is None
