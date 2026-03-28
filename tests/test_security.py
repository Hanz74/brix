"""Tests for SecurityConfig (T-BRIX-V2-16)."""
import pytest
from pathlib import Path
import tempfile

import yaml

from brix.security import SecurityConfig


def make_config(tmp_path: Path, data: dict) -> Path:
    """Write a security.yaml to tmp_path and return the path."""
    config_file = tmp_path / "security.yaml"
    config_file.write_text(yaml.dump(data))
    return config_file


# ---------------------------------------------------------------------------
# No config file — everything allowed
# ---------------------------------------------------------------------------

def test_security_no_config(tmp_path: Path) -> None:
    """Without a config file all checks should pass (open by default)."""
    config_path = tmp_path / "nonexistent.yaml"
    sec = SecurityConfig(config_path=config_path)

    allowed, reason = sec.check_container("any-container")
    assert allowed is True
    assert "No container allowlist" in reason

    allowed, reason = sec.check_executable("any-exe")
    assert allowed is True
    assert "No executable allowlist" in reason

    allowed, reason = sec.check_script_path("/any/path/script.py")
    assert allowed is True
    assert "No script path allowlist" in reason

    allowed, reason = sec.check_shell_mode(True)
    assert allowed is False  # enforce_shell_false defaults to True even without file
    assert "enforce_shell_false" in reason


# ---------------------------------------------------------------------------
# Container allowlist
# ---------------------------------------------------------------------------

def test_security_allowed_container(tmp_path: Path) -> None:
    """Container in the allowlist should be permitted."""
    cfg = make_config(tmp_path, {"allowed_containers": ["m365", "markitdown-mcp"]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_container("m365")
    assert allowed is True
    assert "allowed" in reason


def test_security_blocked_container(tmp_path: Path) -> None:
    """Container not in the allowlist should be blocked."""
    cfg = make_config(tmp_path, {"allowed_containers": ["m365"]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_container("evil-container")
    assert allowed is False
    assert "evil-container" in reason
    assert "not in allowed list" in reason


# ---------------------------------------------------------------------------
# Executable allowlist
# ---------------------------------------------------------------------------

def test_security_allowed_executable(tmp_path: Path) -> None:
    """Executable in the allowlist should be permitted."""
    cfg = make_config(tmp_path, {"allowed_executables": ["python3", "ffmpeg"]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_executable("ffmpeg")
    assert allowed is True
    assert "allowed" in reason


def test_security_blocked_executable(tmp_path: Path) -> None:
    """Executable not in the allowlist should be blocked."""
    cfg = make_config(tmp_path, {"allowed_executables": ["python3"]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_executable("rm")
    assert allowed is False
    assert "not in allowed list" in reason


# ---------------------------------------------------------------------------
# Script path allowlist
# ---------------------------------------------------------------------------

def test_security_script_path_allowed(tmp_path: Path) -> None:
    """Script under an allowed directory should be permitted."""
    scripts_dir = tmp_path / "scripts"
    scripts_dir.mkdir()
    script = scripts_dir / "helper.py"
    script.touch()

    cfg = make_config(tmp_path, {"allowed_script_paths": [str(scripts_dir)]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_script_path(str(script))
    assert allowed is True
    assert "allowed" in reason


def test_security_script_path_blocked(tmp_path: Path) -> None:
    """Script outside allowed directories should be blocked."""
    allowed_dir = tmp_path / "safe"
    allowed_dir.mkdir()
    bad_script = tmp_path / "unsafe" / "attack.py"

    cfg = make_config(tmp_path, {"allowed_script_paths": [str(allowed_dir)]})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_script_path(str(bad_script))
    assert allowed is False
    assert "not under any allowed path" in reason


# ---------------------------------------------------------------------------
# Shell mode enforcement
# ---------------------------------------------------------------------------

def test_security_shell_false_enforced(tmp_path: Path) -> None:
    """shell=True must be blocked when enforce_shell_false is true."""
    cfg = make_config(tmp_path, {"enforce_shell_false": True})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_shell_mode(True)
    assert allowed is False
    assert "enforce_shell_false" in reason


def test_security_shell_true_allowed(tmp_path: Path) -> None:
    """shell=True should be allowed when enforce_shell_false is false."""
    cfg = make_config(tmp_path, {"enforce_shell_false": False})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_shell_mode(True)
    assert allowed is True
    assert "passed" in reason


def test_security_shell_false_always_ok(tmp_path: Path) -> None:
    """shell=False is always safe regardless of policy."""
    cfg = make_config(tmp_path, {"enforce_shell_false": True})
    sec = SecurityConfig(config_path=cfg)

    allowed, reason = sec.check_shell_mode(False)
    assert allowed is True


# ---------------------------------------------------------------------------
# Empty allowlists → everything allowed
# ---------------------------------------------------------------------------

def test_security_empty_allowlist(tmp_path: Path) -> None:
    """An empty allowlist means no restrictions (open by default)."""
    cfg = make_config(
        tmp_path,
        {
            "allowed_containers": [],
            "allowed_executables": [],
            "allowed_script_paths": [],
        },
    )
    sec = SecurityConfig(config_path=cfg)

    allowed, _ = sec.check_container("any-container")
    assert allowed is True

    allowed, _ = sec.check_executable("any-exe")
    assert allowed is True

    allowed, _ = sec.check_script_path("/any/path/script.py")
    assert allowed is True
