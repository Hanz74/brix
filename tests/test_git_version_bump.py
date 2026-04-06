from pathlib import Path

from brix.git_version_bump import bump_version, classify_bump, write_versions


def test_classify_breaking_change_prefixes_and_body() -> None:
    assert classify_bump("feat!: change api") == "major"
    assert classify_bump("fix!: stop supporting old config") == "major"
    assert classify_bump("feat(parser)!: change api") == "major"
    assert classify_bump("feat: add parser\n\nBREAKING CHANGE: config format changed") == "major"


def test_classify_minor_and_patch() -> None:
    assert classify_bump("feat: add option") == "minor"
    assert classify_bump("T-BRIX-V8-12: implement planner") == "minor"
    assert classify_bump("fix: patch behavior") == "patch"
    assert classify_bump("docs: update readme") == "patch"


def test_write_versions_updates_once(tmp_path: Path) -> None:
    pyproject = tmp_path / "pyproject.toml"
    init = tmp_path / "src" / "brix" / "__init__.py"
    init.parent.mkdir(parents=True)
    pyproject.write_text('[project]\nversion = "1.2.3"\n')
    init.write_text('__version__ = "1.2.3"\n')

    old_version, new_version = write_versions("2.0.0", pyproject_path=pyproject, init_path=init) or ("", "")

    assert (old_version, new_version) == ("1.2.3", "2.0.0")
    assert 'version = "2.0.0"' in pyproject.read_text()
    assert '__version__ = "2.0.0"' in init.read_text()

    old_version, new_version = write_versions("2.0.0", pyproject_path=pyproject, init_path=init) or ("", "")

    assert (old_version, new_version) == ("2.0.0", "2.0.0")


def test_bump_version_semver() -> None:
    assert bump_version("1.2.3", "patch") == "1.2.4"
    assert bump_version("1.2.3", "minor") == "1.3.0"
    assert bump_version("1.2.3", "major") == "2.0.0"
