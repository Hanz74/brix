"""Tests for brix.deps — Pipeline-level dependency management (T-BRIX-V4-BUG-11)."""
from __future__ import annotations

import subprocess
import sys
from unittest.mock import MagicMock, patch

import pytest

from brix.deps import _package_name, check_requirements, install_requirements


# ---------------------------------------------------------------------------
# _package_name
# ---------------------------------------------------------------------------


def test_package_name_simple():
    assert _package_name("requests") == "requests"


def test_package_name_with_version_ge():
    assert _package_name("requests>=2.28") == "requests"


def test_package_name_with_version_eq():
    assert _package_name("Pillow==9.0.0") == "Pillow"


def test_package_name_with_version_le():
    assert _package_name("numpy<=1.25") == "numpy"


def test_package_name_with_extras():
    assert _package_name("fastapi[all]") == "fastapi"


def test_package_name_with_marker():
    assert _package_name("pywin32; sys_platform == 'win32'") == "pywin32"


def test_package_name_tilde_eq():
    assert _package_name("urllib3~=1.26") == "urllib3"


# ---------------------------------------------------------------------------
# check_requirements
# ---------------------------------------------------------------------------


def test_check_requirements_empty():
    """Empty list returns empty list."""
    assert check_requirements([]) == []


def test_check_requirements_installed_package():
    """A package that is definitely installed (pip itself) should not be missing."""
    missing = check_requirements(["pip"])
    assert "pip" not in missing


def test_check_requirements_missing_package():
    """A clearly non-existent package is reported as missing."""
    missing = check_requirements(["this-package-definitely-does-not-exist-brix-test-xyz"])
    assert len(missing) == 1
    assert "this-package-definitely-does-not-exist-brix-test-xyz" in missing


def test_check_requirements_mixed():
    """Mix of installed and missing packages."""
    missing = check_requirements(["pip", "this-package-definitely-does-not-exist-brix-test-xyz"])
    assert "pip" not in missing
    assert "this-package-definitely-does-not-exist-brix-test-xyz" in missing


def test_check_requirements_with_version_specifier():
    """Version specifiers are stripped when checking package presence."""
    # pip is installed; the specifier should not matter for existence check
    missing = check_requirements(["pip>=1.0"])
    assert "pip>=1.0" not in missing


def test_check_requirements_dash_underscore_normalisation():
    """Packages with dashes/underscores in name are resolved correctly."""
    # importlib.metadata normalises names; test both forms if installed
    missing = check_requirements(["pyyaml"])
    # pyyaml is a brix dependency and must be present
    assert "pyyaml" not in missing


# ---------------------------------------------------------------------------
# install_requirements
# ---------------------------------------------------------------------------


def test_install_requirements_empty():
    """Empty list is a no-op and returns True."""
    result = install_requirements([])
    assert result is True


def test_install_requirements_success(tmp_path):
    """Successful pip invocation returns True."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        result = install_requirements(["some-package"])
    assert result is True
    mock_run.assert_called_once()
    cmd = mock_run.call_args[0][0]
    assert sys.executable in cmd
    assert "install" in cmd
    assert "--quiet" in cmd
    assert "some-package" in cmd


def test_install_requirements_failure(tmp_path):
    """Failed pip invocation returns False."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=1)
        result = install_requirements(["nonexistent-package"])
    assert result is False


def test_install_requirements_quiet_flag():
    """Quiet=True passes --quiet; quiet=False does not."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        install_requirements(["pkg"], quiet=False)
    cmd = mock_run.call_args[0][0]
    assert "--quiet" not in cmd


def test_install_requirements_extra_args():
    """extra_args are forwarded to pip."""
    with patch("subprocess.run") as mock_run:
        mock_run.return_value = MagicMock(returncode=0)
        install_requirements(["pkg"], extra_args=["--user"])
    cmd = mock_run.call_args[0][0]
    assert "--user" in cmd


# ---------------------------------------------------------------------------
# Pipeline model: requirements field
# ---------------------------------------------------------------------------


def test_pipeline_requirements_field_default():
    """Pipeline model has requirements field defaulting to empty list."""
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="test",
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )
    assert pipeline.requirements == []


def test_pipeline_requirements_field_set():
    """Pipeline model accepts requirements list."""
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="test",
        requirements=["requests>=2.28", "pyyaml"],
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )
    assert pipeline.requirements == ["requests>=2.28", "pyyaml"]


def test_pipeline_requirements_from_yaml(tmp_path):
    """Pipeline YAML with requirements field is loaded correctly."""
    import yaml
    from brix.loader import PipelineLoader

    pipeline_yaml = """\
name: test-deps
version: "1.0.0"
requirements:
  - requests>=2.28
  - pyyaml
steps:
  - id: echo
    type: cli
    args: ["echo", "hello"]
"""
    pipeline_file = tmp_path / "test.yaml"
    pipeline_file.write_text(pipeline_yaml)

    loader = PipelineLoader()
    pipeline = loader.load(str(pipeline_file))
    assert pipeline.requirements == ["requests>=2.28", "pyyaml"]


# ---------------------------------------------------------------------------
# Engine integration
# ---------------------------------------------------------------------------


@pytest.mark.asyncio
async def test_engine_skips_dep_install_when_no_requirements(tmp_path):
    """Engine does NOT call install_requirements when requirements is empty."""
    from brix.engine import PipelineEngine
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="no-deps",
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    with patch("brix.deps.install_requirements") as mock_install:
        engine = PipelineEngine()
        result = await engine.run(pipeline)

    mock_install.assert_not_called()
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_skips_install_when_all_installed(tmp_path):
    """Engine does not install when all requirements are already present."""
    from brix.engine import PipelineEngine
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="all-installed",
        requirements=["pip"],  # pip is always present
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    with patch("brix.deps.install_requirements") as mock_install:
        engine = PipelineEngine()
        result = await engine.run(pipeline)

    mock_install.assert_not_called()
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_installs_missing_requirements():
    """Engine calls install_requirements for missing packages."""
    from brix.engine import PipelineEngine
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="needs-install",
        requirements=["nonexistent-brix-test-pkg"],
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True) as mock_install:
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    mock_install.assert_called_once_with(["nonexistent-brix-test-pkg"])
    assert result.success is True


@pytest.mark.asyncio
async def test_engine_fails_on_install_error():
    """Engine returns failure when package installation fails."""
    from brix.engine import PipelineEngine
    from brix.models import Pipeline, Step

    pipeline = Pipeline(
        name="install-fails",
        requirements=["nonexistent-brix-test-pkg"],
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=False):
            engine = PipelineEngine()
            result = await engine.run(pipeline)

    assert result.success is False
    assert result.steps == {}  # No steps were executed


# ---------------------------------------------------------------------------
# Validator integration
# ---------------------------------------------------------------------------


def test_validator_warns_on_missing_requirements():
    """Validator warns when a required package is not installed."""
    from brix.models import Pipeline, Step
    from brix.validator import PipelineValidator

    pipeline = Pipeline(
        name="test",
        requirements=["this-package-definitely-does-not-exist-brix-test-xyz"],
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    validator = PipelineValidator()
    result = validator.validate(pipeline)

    warning_msgs = " ".join(result.warnings)
    assert "this-package-definitely-does-not-exist-brix-test-xyz" in warning_msgs
    assert "not installed" in warning_msgs or "auto-installed" in warning_msgs


def test_validator_passes_when_requirements_installed():
    """Validator adds a check (not warning) when all requirements are installed."""
    from brix.models import Pipeline, Step
    from brix.validator import PipelineValidator

    pipeline = Pipeline(
        name="test",
        requirements=["pip"],  # always present
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    validator = PipelineValidator()
    result = validator.validate(pipeline)

    # No warning about pip being missing
    warning_msgs = " ".join(result.warnings)
    assert "pip" not in warning_msgs
    # A check should be present confirming requirements satisfied
    check_msgs = " ".join(result.checks)
    assert "requirement" in check_msgs.lower()


def test_validator_no_requirements_no_check():
    """Validator does not emit requirement checks when field is empty."""
    from brix.models import Pipeline, Step
    from brix.validator import PipelineValidator

    pipeline = Pipeline(
        name="test",
        steps=[Step(id="s1", type="cli", args=["echo", "hi"])],
    )

    validator = PipelineValidator()
    result = validator.validate(pipeline)

    check_msgs = " ".join(result.checks)
    assert "requirement" not in check_msgs.lower() or "All 0" not in check_msgs


# ---------------------------------------------------------------------------
# CLI integration
# ---------------------------------------------------------------------------


def test_cli_deps_check_no_requirements(tmp_path):
    """brix deps check exits 0 and reports no requirements defined."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: no-reqs
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    runner = CliRunner()
    result = runner.invoke(main, ["deps", "check", str(pipeline_file)])
    assert result.exit_code == 0
    assert "No requirements" in result.output


def test_cli_deps_check_all_satisfied(tmp_path):
    """brix deps check exits 0 when all requirements are installed."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: with-reqs
requirements:
  - pip
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    runner = CliRunner()
    result = runner.invoke(main, ["deps", "check", str(pipeline_file)])
    assert result.exit_code == 0


def test_cli_deps_check_missing(tmp_path):
    """brix deps check exits 1 when requirements are missing."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: missing-reqs
requirements:
  - this-package-definitely-does-not-exist-brix-test-xyz
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    runner = CliRunner()
    result = runner.invoke(main, ["deps", "check", str(pipeline_file)])
    assert result.exit_code == 1


def test_cli_deps_install_no_requirements(tmp_path):
    """brix deps install exits 0 when no requirements defined."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: no-reqs
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    runner = CliRunner()
    result = runner.invoke(main, ["deps", "install", str(pipeline_file)])
    assert result.exit_code == 0
    assert "No requirements" in result.output


def test_cli_deps_install_already_satisfied(tmp_path):
    """brix deps install exits 0 when all requirements already present."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: satisfied
requirements:
  - pip
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    runner = CliRunner()
    result = runner.invoke(main, ["deps", "install", str(pipeline_file)])
    assert result.exit_code == 0
    assert "already installed" in result.output


def test_cli_deps_install_installs_missing(tmp_path):
    """brix deps install calls pip for missing packages."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: needs-install
requirements:
  - nonexistent-brix-test-pkg
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True):
            runner = CliRunner()
            result = runner.invoke(main, ["deps", "install", str(pipeline_file)])

    assert result.exit_code == 0
    assert "installed successfully" in result.output


def test_cli_deps_install_fails(tmp_path):
    """brix deps install exits 1 when pip installation fails."""
    from click.testing import CliRunner
    from brix.cli import main

    pipeline_yaml = """\
name: install-fails
requirements:
  - nonexistent-brix-test-pkg
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=False):
            runner = CliRunner()
            result = runner.invoke(main, ["deps", "install", str(pipeline_file)])

    assert result.exit_code == 1


# ---------------------------------------------------------------------------
# Bundle integration
# ---------------------------------------------------------------------------


def test_bundle_manifest_includes_requirements():
    """BundleManifest serialises requirements field correctly."""
    from brix.bundle import BundleManifest

    manifest = BundleManifest(
        pipeline_name="test",
        pipeline_file="pipeline.yaml",
        brix_version="4.6.0",
        created_at="2026-01-01T00:00:00+00:00",
        helpers=[],
        requirements=["requests>=2.28", "pyyaml"],
    )
    d = manifest.to_dict()
    assert d["requirements"] == ["requests>=2.28", "pyyaml"]


def test_bundle_manifest_from_dict_requirements():
    """BundleManifest.from_dict round-trips requirements."""
    from brix.bundle import BundleManifest

    data = {
        "pipeline_name": "test",
        "pipeline_file": "pipeline.yaml",
        "brix_version": "4.6.0",
        "created_at": "2026-01-01T00:00:00+00:00",
        "helpers": [],
        "requirements": ["click>=8.0"],
    }
    manifest = BundleManifest.from_dict(data)
    assert manifest.requirements == ["click>=8.0"]


def test_bundle_manifest_from_dict_no_requirements():
    """BundleManifest.from_dict defaults requirements to empty list."""
    from brix.bundle import BundleManifest

    data = {
        "pipeline_name": "test",
        "pipeline_file": "pipeline.yaml",
        "brix_version": "4.6.0",
        "created_at": "",
        "helpers": [],
    }
    manifest = BundleManifest.from_dict(data)
    assert manifest.requirements == []


def test_export_bundle_includes_requirements(tmp_path):
    """export_bundle stores pipeline requirements in manifest.json."""
    import json
    import tarfile as tf

    pipeline_yaml = """\
name: dep-pipeline
version: "1.0.0"
requirements:
  - requests>=2.28
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    pipeline_file = tmp_path / "pipeline.yaml"
    pipeline_file.write_text(pipeline_yaml)
    output_path = tmp_path / "pipeline.brix.tar.gz"

    from brix.bundle import export_bundle
    export_bundle(pipeline_file, output_path)

    with tf.open(output_path, "r:gz") as tar:
        members = {m.name: m for m in tar.getmembers()}
        assert "manifest.json" in members
        f = tar.extractfile(members["manifest.json"])
        manifest_data = json.loads(f.read())

    assert manifest_data["requirements"] == ["requests>=2.28"]


def test_import_result_has_requirement_fields():
    """ImportResult has installed_requirements and failed_requirements fields."""
    from brix.bundle import ImportResult

    r = ImportResult(pipeline=None, helpers=[], manifest=None)
    assert r.installed_requirements == []
    assert r.failed_requirements == []


def test_import_bundle_installs_requirements(tmp_path):
    """import_bundle auto-installs requirements listed in the manifest."""
    import json
    import tarfile as tf
    from io import BytesIO

    # Build a minimal bundle with requirements in manifest
    pipeline_yaml = """\
name: dep-import
version: "1.0.0"
requirements:
  - nonexistent-brix-test-pkg
steps:
  - id: s1
    type: cli
    args: ["echo", "hi"]
"""
    manifest_data = {
        "pipeline_name": "dep-import",
        "pipeline_file": "pipeline.yaml",
        "brix_version": "4.6.0",
        "created_at": "2026-01-01T00:00:00+00:00",
        "helpers": [],
        "missing_helpers": [],
        "pipeline_checksum": "",
        "requirements": ["nonexistent-brix-test-pkg"],
    }

    bundle_path = tmp_path / "test.brix.tar.gz"
    with tf.open(bundle_path, "w:gz") as tar:
        for name, data in [
            ("pipeline.yaml", pipeline_yaml.encode()),
            ("manifest.json", json.dumps(manifest_data).encode()),
        ]:
            info = tf.TarInfo(name=name)
            info.size = len(data)
            tar.addfile(info, BytesIO(data))

    pipelines_dir = tmp_path / "pipelines"
    helpers_dir = tmp_path / "helpers"

    with patch("brix.deps.check_requirements", return_value=["nonexistent-brix-test-pkg"]):
        with patch("brix.deps.install_requirements", return_value=True) as mock_install:
            from brix.bundle import import_bundle
            result = import_bundle(
                bundle_path,
                pipelines_dir=pipelines_dir,
                helpers_dir=helpers_dir,
                overwrite=True,
            )

    mock_install.assert_called_once_with(["nonexistent-brix-test-pkg"])
    assert result.installed_requirements == ["nonexistent-brix-test-pkg"]
    assert result.failed_requirements == []
