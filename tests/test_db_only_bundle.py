import io
import json
import tarfile

import yaml

from brix.bundle import MANIFEST_NAME, export_bundle, export_project, import_bundle, import_project
from brix.db import BrixDB
from brix.pipeline_store import PipelineStore


PIPELINE_DATA = {
    "name": "db-only-pipeline",
    "version": "1.0.0",
    "description": "Pipeline stored in normalized DB rows",
    "project": "dbproj",
    "credentials": {
        "api_key": {"env": "API_KEY"},
    },
    "steps": [
        {"id": "fetch", "type": "http.request", "url": "https://example.com/api"},
        {
            "id": "transform",
            "type": "script.python",
            "script": "helpers/normalize_payload.py",
            "helper": "normalize_payload",
            "params": {"mode": "strict"},
        },
    ],
}


def _step_count(db: BrixDB, pipeline_name: str) -> int:
    pipeline = db.get_pipeline(pipeline_name)
    assert pipeline is not None
    with db._connect() as conn:
        row = conn.execute(
            "SELECT COUNT(*) FROM pipeline_step WHERE pipeline_id=?",
            (pipeline["id"],),
        ).fetchone()
    return int(row[0])


def _pipeline_yaml_from_archive(archive_path, member_name: str) -> dict:
    with tarfile.open(archive_path, "r:gz") as tar:
        payload = tar.extractfile(member_name)
        assert payload is not None
        return yaml.safe_load(payload.read().decode("utf-8"))


def _seed_pipeline(db: BrixDB) -> PipelineStore:
    store = PipelineStore(db=db)
    store.save(dict(PIPELINE_DATA))
    db.upsert_helper(
        name="normalize_payload",
        script_path="db://normalize_payload",
        description="Normalizer",
        code="# helper\nprint('ok')\n",
    )
    return store


def test_export_project_writes_yaml_generated_from_db_rows(tmp_path):
    db = BrixDB(db_path=tmp_path / "source.db")
    _seed_pipeline(db)

    with db._connect() as conn:
        conn.execute(
            "UPDATE pipeline SET yaml_content=? WHERE name=?",
            ("name: stale-pipeline\nsteps: []\n", "db-only-pipeline"),
        )

    archive_path = tmp_path / "dbproj.project.brix.tar.gz"
    manifest = export_project("dbproj", archive_path, db=db)

    assert archive_path.exists()
    assert manifest.counts["pipelines"] == 1

    exported = _pipeline_yaml_from_archive(archive_path, "pipelines/db-only-pipeline.yaml")
    assert exported["name"] == "db-only-pipeline"
    assert exported["steps"][1]["helper"] == "normalize_payload"
    assert exported["credentials"] == {"api_key": {"env": "API_KEY"}}

    with tarfile.open(archive_path, "r:gz") as tar:
        manifest_member = tar.extractfile(MANIFEST_NAME)
        assert manifest_member is not None
        manifest_payload = json.loads(manifest_member.read().decode("utf-8"))
    assert manifest_payload["credential_references"] == {"db-only-pipeline": ["api_key"]}


def test_import_project_creates_pipeline_step_rows(tmp_path):
    source_db = BrixDB(db_path=tmp_path / "source.db")
    _seed_pipeline(source_db)
    archive_path = tmp_path / "dbproj.project.brix.tar.gz"
    export_project("dbproj", archive_path, db=source_db)

    target_db = BrixDB(db_path=tmp_path / "target.db")
    result = import_project(archive_path, db=target_db)

    assert result.errors == []
    assert result.imported["pipelines"] == 1
    assert _step_count(target_db, "db-only-pipeline") == 2
    assert target_db.get_pipeline("db-only-pipeline")["migration_status"] == "v71_complete"


def test_bundle_roundtrip_preserves_pipeline_on_fresh_db(tmp_path):
    source_db = BrixDB(db_path=tmp_path / "source.db")
    _seed_pipeline(source_db)

    pipeline_ref = tmp_path / "db-only-pipeline.yaml"
    pipeline_ref.write_text("name: wrong-from-disk\nsteps: []\n", encoding="utf-8")

    bundle_path = tmp_path / "db-only-pipeline.brix.tar.gz"
    export_bundle(pipeline_ref, bundle_path, base_dir=tmp_path, db=source_db)

    exported = _pipeline_yaml_from_archive(bundle_path, "pipeline.yaml")
    assert exported["name"] == "db-only-pipeline"
    assert exported["steps"][1]["script"] == "helpers/normalize_payload.py"

    target_db = BrixDB(db_path=tmp_path / "target.db")
    result = import_bundle(
        bundle_path,
        pipelines_dir=tmp_path / "virtual-pipelines",
        helpers_dir=tmp_path / "helpers",
        db=target_db,
    )

    assert result.pipeline is not None
    assert _step_count(target_db, "db-only-pipeline") == 2

    source_row = source_db.get_pipeline("db-only-pipeline")
    target_row = target_db.get_pipeline("db-only-pipeline")
    assert source_row is not None
    assert target_row is not None
    assert source_db.pipeline_to_dict(source_row["id"]) == target_db.pipeline_to_dict(target_row["id"])


def test_import_bundle_accepts_pre_migration_yaml_only_archive(tmp_path):
    bundle_path = tmp_path / "legacy.brix.tar.gz"
    legacy_yaml = yaml.dump(PIPELINE_DATA, sort_keys=False).encode("utf-8")
    legacy_manifest = json.dumps(
        {
            "pipeline_name": "db-only-pipeline",
            "pipeline_file": "db-only-pipeline.yaml",
            "brix_version": "legacy",
            "created_at": "",
            "helpers": [],
            "missing_helpers": [],
            "pipeline_checksum": "",
            "requirements": [],
        }
    ).encode("utf-8")

    with tarfile.open(bundle_path, "w:gz") as tar:
        pipeline_info = tarfile.TarInfo(name="pipeline.yaml")
        pipeline_info.size = len(legacy_yaml)
        tar.addfile(pipeline_info, io.BytesIO(legacy_yaml))

        manifest_info = tarfile.TarInfo(name="manifest.json")
        manifest_info.size = len(legacy_manifest)
        tar.addfile(manifest_info, io.BytesIO(legacy_manifest))

    db = BrixDB(db_path=tmp_path / "target.db")
    result = import_bundle(
        bundle_path,
        pipelines_dir=tmp_path / "virtual-pipelines",
        helpers_dir=tmp_path / "helpers",
        db=db,
    )

    assert result.pipeline is not None
    assert _step_count(db, "db-only-pipeline") == 2
    assert db.get_pipeline("db-only-pipeline")["migration_status"] == "v71_complete"
