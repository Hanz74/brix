"""Tests for T-BRIX-V4-19 — Pipeline Import/Export (bundle module)."""
import json
import tarfile
from pathlib import Path

import pytest
import yaml

from brix.bundle import (
    BUNDLE_SUFFIX,
    MANIFEST_NAME,
    PIPELINE_NAME_IN_BUNDLE,
    BundleManifest,
    ImportResult,
    export_bundle,
    find_helper_references,
    import_bundle,
    read_manifest,
)


# ---------------------------------------------------------------------------
# Fixtures
# ---------------------------------------------------------------------------

SIMPLE_PIPELINE = """\
name: simple-pipeline
version: "1.0.0"
description: A simple test pipeline
steps:
  - id: echo
    type: cli
    args: ["echo", "hello"]
"""

PIPELINE_WITH_HELPERS = """\
name: helper-pipeline
version: "1.0.0"
steps:
  - id: fetch
    type: cli
    args: ["echo", "data"]
  - id: process
    type: python
    script: helpers/process.py
    params:
      input: "{{ fetch.output }}"
  - id: save
    type: python
    script: helpers/save_results.py
    params:
      data: "{{ process.output }}"
"""

PIPELINE_WITH_PARAMS_HELPER = """\
name: param-ref-pipeline
version: "1.0.0"
steps:
  - id: run
    type: python
    script: helpers/runner.py
    params:
      script_path: helpers/extra.py
"""


# ---------------------------------------------------------------------------
# find_helper_references
# ---------------------------------------------------------------------------

class TestFindHelperReferences:
    def test_no_helpers(self):
        refs = find_helper_references(SIMPLE_PIPELINE)
        assert refs == []

    def test_script_fields(self):
        refs = find_helper_references(PIPELINE_WITH_HELPERS)
        assert "helpers/process.py" in refs
        assert "helpers/save_results.py" in refs
        assert len(refs) == 2

    def test_param_value_reference(self):
        refs = find_helper_references(PIPELINE_WITH_PARAMS_HELPER)
        # helpers/runner.py from script field + helpers/extra.py from param value
        assert "helpers/runner.py" in refs
        assert "helpers/extra.py" in refs

    def test_sorted_output(self):
        refs = find_helper_references(PIPELINE_WITH_HELPERS)
        assert refs == sorted(refs)

    def test_deduplicated(self):
        yaml_with_dups = """\
name: dup
steps:
  - id: a
    type: python
    script: helpers/same.py
  - id: b
    type: python
    script: helpers/same.py
"""
        refs = find_helper_references(yaml_with_dups)
        assert refs.count("helpers/same.py") == 1

    def test_ignores_non_helper_scripts(self):
        yaml_content = """\
name: other
steps:
  - id: a
    type: python
    script: /abs/path/script.py
"""
        refs = find_helper_references(yaml_content)
        assert refs == []


# ---------------------------------------------------------------------------
# export_bundle
# ---------------------------------------------------------------------------

class TestExportBundle:
    def test_creates_archive(self, tmp_path):
        pipeline_file = tmp_path / "simple.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "simple.brix.tar.gz"

        manifest = export_bundle(pipeline_file, out)

        assert out.exists()
        assert isinstance(manifest, BundleManifest)
        assert manifest.pipeline_name == "simple-pipeline"

    def test_archive_contains_pipeline_yaml(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "p.brix.tar.gz"
        export_bundle(pipeline_file, out)

        with tarfile.open(out, "r:gz") as tar:
            names = tar.getnames()
        assert PIPELINE_NAME_IN_BUNDLE in names

    def test_archive_contains_manifest(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "p.brix.tar.gz"
        export_bundle(pipeline_file, out)

        with tarfile.open(out, "r:gz") as tar:
            names = tar.getnames()
        assert MANIFEST_NAME in names

    def test_manifest_contents(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "p.brix.tar.gz"
        manifest = export_bundle(pipeline_file, out)

        assert manifest.helpers == []
        assert manifest.pipeline_name == "simple-pipeline"
        assert manifest.brix_version != ""
        assert manifest.created_at != ""
        assert manifest.pipeline_checksum != ""

    def test_bundles_helpers(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "process.py").write_text("# process")
        (helpers_dir / "save_results.py").write_text("# save")
        out = tmp_path / "bundle.brix.tar.gz"

        manifest = export_bundle(pipeline_file, out, base_dir=tmp_path)

        assert "helpers/process.py" in manifest.helpers
        assert "helpers/save_results.py" in manifest.helpers

        with tarfile.open(out, "r:gz") as tar:
            names = tar.getnames()
        assert "helpers/process.py" in names
        assert "helpers/save_results.py" in names

    def test_raises_on_missing_helper(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        # No helpers directory created
        out = tmp_path / "bundle.brix.tar.gz"

        with pytest.raises(FileNotFoundError, match="helpers/process.py"):
            export_bundle(pipeline_file, out, base_dir=tmp_path)

    def test_include_missing_skips_helpers(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        out = tmp_path / "bundle.brix.tar.gz"

        manifest = export_bundle(
            pipeline_file, out, base_dir=tmp_path, include_missing=True
        )

        assert "helpers/process.py" in manifest.missing_helpers
        assert "helpers/save_results.py" in manifest.missing_helpers
        assert manifest.helpers == []
        assert out.exists()

    def test_default_output_path(self, tmp_path):
        pipeline_file = tmp_path / "my-pipe.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        expected_out = tmp_path / f"my-pipe{BUNDLE_SUFFIX}"

        manifest = export_bundle(pipeline_file, expected_out)
        assert expected_out.exists()


# ---------------------------------------------------------------------------
# import_bundle
# ---------------------------------------------------------------------------

class TestImportBundle:
    def _make_bundle(self, tmp_path, pipeline_yaml=None, helpers=None):
        """Helper: create a bundle file and return its path."""
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        pipeline_file = src_dir / "pipeline.yaml"
        pipeline_file.write_text(pipeline_yaml or SIMPLE_PIPELINE)

        if helpers:
            h_dir = src_dir / "helpers"
            h_dir.mkdir()
            for name, content in helpers.items():
                (h_dir / name).write_text(content)

        bundle_path = tmp_path / "bundle.brix.tar.gz"
        export_bundle(pipeline_file, bundle_path, base_dir=src_dir, include_missing=True)
        return bundle_path

    def test_installs_pipeline(self, tmp_path):
        bundle_path = self._make_bundle(tmp_path)
        dest_pipelines = tmp_path / "pipelines"

        result = import_bundle(bundle_path, pipelines_dir=dest_pipelines)

        assert result.pipeline is not None
        assert result.pipeline.exists()
        assert result.pipeline.name == "simple-pipeline.yaml"

    def test_installed_pipeline_content(self, tmp_path):
        bundle_path = self._make_bundle(tmp_path)
        dest_pipelines = tmp_path / "pipelines"

        result = import_bundle(bundle_path, pipelines_dir=dest_pipelines)
        content = result.pipeline.read_text()
        assert "simple-pipeline" in content

    def test_installs_helpers(self, tmp_path):
        bundle_path = self._make_bundle(
            tmp_path,
            pipeline_yaml=PIPELINE_WITH_HELPERS,
            helpers={"process.py": "# process", "save_results.py": "# save"},
        )
        dest_pipelines = tmp_path / "pipelines"
        dest_helpers = tmp_path / "helpers"

        result = import_bundle(
            bundle_path,
            pipelines_dir=dest_pipelines,
            helpers_dir=dest_helpers,
        )

        assert len(result.helpers) == 2
        helper_names = [h.name for h in result.helpers]
        assert "process.py" in helper_names
        assert "save_results.py" in helper_names
        for h in result.helpers:
            assert h.exists()

    def test_raises_on_existing_pipeline_without_overwrite(self, tmp_path):
        bundle_path = self._make_bundle(tmp_path)
        dest_pipelines = tmp_path / "pipelines"
        dest_pipelines.mkdir()
        # Pre-create the pipeline file
        (dest_pipelines / "simple-pipeline.yaml").write_text("existing content")

        with pytest.raises(FileExistsError, match="Pipeline already exists"):
            import_bundle(bundle_path, pipelines_dir=dest_pipelines)

    def test_overwrite_replaces_existing(self, tmp_path):
        bundle_path = self._make_bundle(tmp_path)
        dest_pipelines = tmp_path / "pipelines"
        dest_pipelines.mkdir()
        (dest_pipelines / "simple-pipeline.yaml").write_text("old content")

        result = import_bundle(
            bundle_path, pipelines_dir=dest_pipelines, overwrite=True
        )
        content = result.pipeline.read_text()
        assert "old content" not in content
        assert "simple-pipeline" in content

    def test_raises_on_existing_helper_without_overwrite(self, tmp_path):
        bundle_path = self._make_bundle(
            tmp_path,
            pipeline_yaml=PIPELINE_WITH_HELPERS,
            helpers={"process.py": "# new"},
        )
        dest_pipelines = tmp_path / "pipelines"
        dest_helpers = tmp_path / "helpers"
        dest_helpers.mkdir()
        (dest_helpers / "process.py").write_text("# existing")

        with pytest.raises(FileExistsError, match="Helper already exists"):
            import_bundle(
                bundle_path,
                pipelines_dir=dest_pipelines,
                helpers_dir=dest_helpers,
            )

    def test_manifest_attached_to_result(self, tmp_path):
        bundle_path = self._make_bundle(tmp_path)
        dest_pipelines = tmp_path / "pipelines"

        result = import_bundle(bundle_path, pipelines_dir=dest_pipelines)

        assert result.manifest is not None
        assert result.manifest.pipeline_name == "simple-pipeline"

    def test_invalid_bundle_no_pipeline_yaml(self, tmp_path):
        bundle_path = tmp_path / "bad.brix.tar.gz"
        with tarfile.open(bundle_path, "w:gz") as tar:
            # Write only the manifest, no pipeline.yaml
            data = json.dumps({"pipeline_name": "x"}).encode()
            info = tarfile.TarInfo(name=MANIFEST_NAME)
            info.size = len(data)
            import io
            tar.addfile(info, io.BytesIO(data))

        with pytest.raises(ValueError, match="pipeline.yaml"):
            import_bundle(bundle_path, pipelines_dir=tmp_path / "p")


# ---------------------------------------------------------------------------
# read_manifest
# ---------------------------------------------------------------------------

class TestReadManifest:
    def test_reads_manifest_from_bundle(self, tmp_path):
        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "p.brix.tar.gz"
        export_bundle(pipeline_file, out)

        manifest = read_manifest(out)

        assert manifest is not None
        assert manifest.pipeline_name == "simple-pipeline"

    def test_returns_none_if_no_manifest(self, tmp_path):
        bundle_path = tmp_path / "no-manifest.brix.tar.gz"
        with tarfile.open(bundle_path, "w:gz") as tar:
            data = b"name: x\nsteps: []\n"
            info = tarfile.TarInfo(name=PIPELINE_NAME_IN_BUNDLE)
            info.size = len(data)
            import io
            tar.addfile(info, io.BytesIO(data))

        manifest = read_manifest(bundle_path)
        assert manifest is None


# ---------------------------------------------------------------------------
# BundleManifest serialisation
# ---------------------------------------------------------------------------

class TestBundleManifest:
    def test_round_trip(self):
        m = BundleManifest(
            pipeline_name="my-pipe",
            pipeline_file="my-pipe.yaml",
            brix_version="4.4.0",
            created_at="2026-01-01T00:00:00+00:00",
            helpers=["helpers/a.py", "helpers/b.py"],
            missing_helpers=["helpers/c.py"],
            pipeline_checksum="abc123",
        )
        d = m.to_dict()
        m2 = BundleManifest.from_dict(d)

        assert m2.pipeline_name == m.pipeline_name
        assert m2.brix_version == m.brix_version
        assert m2.helpers == m.helpers
        assert m2.missing_helpers == m.missing_helpers
        assert m2.pipeline_checksum == m.pipeline_checksum

    def test_from_dict_with_missing_keys(self):
        m = BundleManifest.from_dict({})
        assert m.pipeline_name == "unknown"
        assert m.helpers == []
        assert m.missing_helpers == []


# ---------------------------------------------------------------------------
# CLI integration tests
# ---------------------------------------------------------------------------

class TestBundleCLI:
    def test_bundle_export_command(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "out.brix.tar.gz"

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["bundle", "export", str(pipeline_file), "--output", str(out)],
        )
        assert result.exit_code == 0, result.output
        assert "Bundle created" in result.output
        assert out.exists()

    def test_bundle_export_default_output_name(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "my-flow.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        expected = tmp_path / f"my-flow{BUNDLE_SUFFIX}"

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["bundle", "export", str(pipeline_file)],
        )
        assert result.exit_code == 0, result.output
        assert expected.exists()

    def test_bundle_export_shows_helpers(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "process.py").write_text("# process")
        (helpers_dir / "save_results.py").write_text("# save")
        out = tmp_path / "bundle.brix.tar.gz"

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["bundle", "export", str(pipeline_file), "--output", str(out)],
        )
        assert result.exit_code == 0, result.output
        assert "helpers/process.py" in result.output
        assert "helpers/save_results.py" in result.output

    def test_bundle_export_missing_helper_error(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        out = tmp_path / "bundle.brix.tar.gz"

        runner = CliRunner()
        result = runner.invoke(
            main,
            ["bundle", "export", str(pipeline_file), "--output", str(out)],
        )
        assert result.exit_code != 0
        assert "✗" in result.output

    def test_bundle_export_include_missing_flag(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        out = tmp_path / "bundle.brix.tar.gz"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle", "export", str(pipeline_file),
                "--output", str(out),
                "--include-missing",
            ],
        )
        assert result.exit_code == 0, result.output
        assert out.exists()

    def test_bundle_import_command(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        # First create a bundle
        src_dir = tmp_path / "src"
        src_dir.mkdir()
        pf = src_dir / "p.yaml"
        pf.write_text(SIMPLE_PIPELINE)
        bundle_path = tmp_path / "bundle.brix.tar.gz"
        export_bundle(pf, bundle_path)

        dest_pipelines = tmp_path / "dest_pipelines"

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle", "import", str(bundle_path),
                "--pipelines-dir", str(dest_pipelines),
                "--helpers-dir", str(tmp_path / "dest_helpers"),
            ],
        )
        assert result.exit_code == 0, result.output
        assert "Bundle imported" in result.output
        assert (dest_pipelines / "simple-pipeline.yaml").exists()

    def test_bundle_import_overwrite_flag(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        src_dir = tmp_path / "src"
        src_dir.mkdir()
        pf = src_dir / "p.yaml"
        pf.write_text(SIMPLE_PIPELINE)
        bundle_path = tmp_path / "bundle.brix.tar.gz"
        export_bundle(pf, bundle_path)

        dest_pipelines = tmp_path / "dest_pipelines"
        dest_pipelines.mkdir()
        (dest_pipelines / "simple-pipeline.yaml").write_text("old")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle", "import", str(bundle_path),
                "--pipelines-dir", str(dest_pipelines),
                "--overwrite",
            ],
        )
        assert result.exit_code == 0, result.output

    def test_bundle_import_no_overwrite_fails(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        src_dir = tmp_path / "src"
        src_dir.mkdir()
        pf = src_dir / "p.yaml"
        pf.write_text(SIMPLE_PIPELINE)
        bundle_path = tmp_path / "bundle.brix.tar.gz"
        export_bundle(pf, bundle_path)

        dest_pipelines = tmp_path / "dest_pipelines"
        dest_pipelines.mkdir()
        (dest_pipelines / "simple-pipeline.yaml").write_text("old")

        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "bundle", "import", str(bundle_path),
                "--pipelines-dir", str(dest_pipelines),
            ],
        )
        assert result.exit_code != 0

    def test_bundle_inspect_command(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(SIMPLE_PIPELINE)
        out = tmp_path / "p.brix.tar.gz"
        export_bundle(pipeline_file, out)

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "inspect", str(out)])
        assert result.exit_code == 0, result.output
        assert "simple-pipeline" in result.output
        assert PIPELINE_NAME_IN_BUNDLE in result.output
        assert MANIFEST_NAME in result.output

    def test_bundle_inspect_shows_helpers(self, tmp_path):
        from click.testing import CliRunner
        from brix.cli import main

        pipeline_file = tmp_path / "p.yaml"
        pipeline_file.write_text(PIPELINE_WITH_HELPERS)
        helpers_dir = tmp_path / "helpers"
        helpers_dir.mkdir()
        (helpers_dir / "process.py").write_text("# p")
        (helpers_dir / "save_results.py").write_text("# s")
        out = tmp_path / "bundle.brix.tar.gz"
        export_bundle(pipeline_file, out, base_dir=tmp_path)

        runner = CliRunner()
        result = runner.invoke(main, ["bundle", "inspect", str(out)])
        assert result.exit_code == 0, result.output
        assert "helpers/process.py" in result.output
        assert "helpers/save_results.py" in result.output
