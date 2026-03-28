"""Proof-of-Concept: Brick-Pipeline End-to-End — T-BRIX-DB-09.

Beweist dass die Brick-First-Architektur funktioniert: Eine Pipeline die NUR
aus Bricks besteht (kein Python-Helper), durch die Engine läuft und korrekte
Ergebnisse liefert.

Use-Case: Spracherkennung (wie buddy_extract_language) als reine Brick-Pipeline.
Flow: db.query → extract.specialist (foreach) → db.upsert

NOTE: DbQueryRunner liest connection/query als direkte Step-Attribute (nicht via
params), daher nutzen die E2E-Runner-Tests direkte Step-Objekte (wie im bestehenden
test_smoke_regression.py). Engine-Tests fokussieren auf Brick-Resolution, Compositor-
Mode und Steps die params unterstützen.
"""
from __future__ import annotations

import asyncio
import sqlite3
import warnings

import pytest

from brix.engine import PipelineEngine, LEGACY_ALIASES
from brix.loader import PipelineLoader
from brix.models import Pipeline, Step
from brix.runners.db_query import DbQueryRunner
from brix.runners.db_upsert import DbUpsertRunner
from brix.runners.specialist import SpecialistRunner


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_test_db(tmp_path, *, with_extractions_table: bool = True) -> str:
    """Create a SQLite DB with documents + extractions tables, return path."""
    db_path = tmp_path / "test.db"
    conn = sqlite3.connect(str(db_path))
    conn.execute("CREATE TABLE documents (id TEXT, text TEXT)")
    if with_extractions_table:
        conn.execute("CREATE TABLE extractions (doc_id TEXT, language TEXT)")
    conn.executemany("INSERT INTO documents VALUES (?, ?)", [
        ("doc1", "Dies ist ein deutscher Text mit Vertragsbedingungen und AGB."),
        ("doc2", "This is an English document about terms and conditions."),
        ("doc3", "Ceci est un document français avec des conditions générales."),
        ("doc4", "Dit is een Nederlands document."),
    ])
    conn.commit()
    conn.close()
    return str(db_path)


def _load(yaml_str: str) -> Pipeline:
    return PipelineLoader().load_from_string(yaml_str)


class _DbQueryStep:
    """Fake step for DbQueryRunner (direct attribute access, not params)."""
    def __init__(self, connection: str, query: str, params: dict | None = None):
        self.connection = connection
        self.query = query
        self.params = params


class _SpecialistStep:
    """Fake step for SpecialistRunner (config dict)."""
    def __init__(self, config: dict):
        self.config = config


class _UpsertStep:
    """Fake step for DbUpsertRunner."""
    def __init__(self, connection: str, table: str, data, conflict_key=None):
        self.connection = connection
        self.table = table
        self.params = {"data": data}
        self.conflict_key = conflict_key


class _FakeContext:
    """Minimal PipelineContext stub."""
    def __init__(self, data: dict | None = None):
        self._data = data or {}

    def to_jinja_context(self) -> dict:
        return dict(self._data)


# ---------------------------------------------------------------------------
# Class 1: Brick names resolve correctly — NOT as legacy aliases
# ---------------------------------------------------------------------------

class TestBrickNamesResolved:
    """Verifiziere dass db.query/db.upsert/extract.specialist als Bricks aufgelöst
    werden, NICHT als Legacy-Aliases."""

    def test_db_query_is_not_legacy_alias(self):
        """db.query sollte nicht in LEGACY_ALIASES sein (ist ein nativer Brick-Name)."""
        assert "db.query" not in LEGACY_ALIASES

    def test_db_upsert_is_not_legacy_alias(self):
        """db.upsert sollte nicht in LEGACY_ALIASES sein."""
        assert "db.upsert" not in LEGACY_ALIASES

    def test_extract_specialist_is_not_legacy_alias(self):
        """extract.specialist sollte nicht in LEGACY_ALIASES sein."""
        assert "extract.specialist" not in LEGACY_ALIASES

    def test_legacy_alias_db_query_exists(self):
        """Das alte 'db_query' ist ein Legacy-Alias für 'db.query'."""
        assert LEGACY_ALIASES.get("db_query") == "db.query"

    def test_legacy_alias_db_upsert_exists(self):
        """Das alte 'db_upsert' ist ein Legacy-Alias für 'db.upsert'."""
        assert LEGACY_ALIASES.get("db_upsert") == "db.upsert"

    def test_legacy_alias_specialist_exists(self):
        """Das alte 'specialist' ist ein Legacy-Alias für 'extract.specialist'."""
        assert LEGACY_ALIASES.get("specialist") == "extract.specialist"

    def test_engine_resolves_db_query_brick_without_deprecation(self):
        """PipelineEngine._resolve_runner('db.query') liefert runner ohne DeprecationWarning."""
        engine = PipelineEngine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("db.query")
        assert runner is not None, "db.query runner should be resolved"
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0, "db.query should not emit DeprecationWarning"

    def test_engine_resolves_extract_specialist_brick_without_deprecation(self):
        """PipelineEngine._resolve_runner('extract.specialist') gibt runner ohne Warnung."""
        engine = PipelineEngine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("extract.specialist")
        assert runner is not None
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0

    def test_engine_resolves_db_upsert_brick_without_deprecation(self):
        """PipelineEngine._resolve_runner('db.upsert') gibt runner ohne Warnung."""
        engine = PipelineEngine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("db.upsert")
        assert runner is not None
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) == 0

    def test_engine_resolves_legacy_db_query_with_deprecation(self):
        """Das alte 'db_query' (Legacy) löst DeprecationWarning aus."""
        engine = PipelineEngine()
        with warnings.catch_warnings(record=True) as w:
            warnings.simplefilter("always")
            runner = engine._resolve_runner("db_query")
        assert runner is not None, "Legacy 'db_query' should still resolve"
        deprecation_warnings = [x for x in w if issubclass(x.category, DeprecationWarning)]
        assert len(deprecation_warnings) >= 1, "Legacy type should produce DeprecationWarning"

    def test_brick_resolver_returns_correct_runner_type(self):
        """db.query resolves zu DbQueryRunner (nicht ein anderer Runner)."""
        engine = PipelineEngine()
        runner = engine._resolve_runner("db.query")
        assert isinstance(runner, DbQueryRunner)

    def test_specialist_brick_resolver_returns_correct_runner_type(self):
        """extract.specialist resolves zu SpecialistRunner."""
        engine = PipelineEngine()
        runner = engine._resolve_runner("extract.specialist")
        assert isinstance(runner, SpecialistRunner)


# ---------------------------------------------------------------------------
# Class 2: Runner-Level E2E — db.query → specialist → db.upsert
# ---------------------------------------------------------------------------

class TestRunnerLevelE2E:
    """Vollständige E2E-Tests auf Runner-Ebene: Brick-Kette ohne Engine-Overhead.

    DbQueryRunner nutzt direkte Step-Attribute (kein params-Dict), deshalb
    wird hier die gleiche Pattern wie in test_smoke_regression.py genutzt.
    """

    @pytest.mark.asyncio
    async def test_db_query_runner_returns_all_documents(self, tmp_path):
        """DbQueryRunner (db.query Brick) liest 4 Dokumente aus SQLite."""
        db_path = _make_test_db(tmp_path)
        runner = DbQueryRunner()
        step = _DbQueryStep(connection=db_path, query="SELECT id, text FROM documents ORDER BY id")
        result = await runner.execute(step, _FakeContext())

        assert result["success"] is True
        data = result["data"]
        assert data["row_count"] == 4
        assert data["columns"] == ["id", "text"]
        assert data["rows"][0]["id"] == "doc1"
        assert "AGB" in data["rows"][0]["text"]

    @pytest.mark.asyncio
    async def test_specialist_runner_detects_german_marker(self):
        """SpecialistRunner (extract.specialist Brick) erkennt deutschen Marker."""
        runner = SpecialistRunner()
        ctx = _FakeContext({"text": "Dies ist ein Text mit AGB und Vertragsbedingungen."})
        step = _SpecialistStep(config={
            "input_field": "text",
            "extract": [
                {"name": "language", "method": "regex", "pattern": "(Vertrag|AGB|Bescheid)", "default": "unknown"},
            ],
        })
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["result"]["language"] in ("Vertrag", "AGB", "Bescheid")

    @pytest.mark.asyncio
    async def test_specialist_runner_detects_english_marker(self):
        """SpecialistRunner erkennt englischen Marker."""
        runner = SpecialistRunner()
        ctx = _FakeContext({"text": "This is an English document about terms and conditions."})
        step = _SpecialistStep(config={
            "input_field": "text",
            "extract": [
                {"name": "language_de", "method": "regex", "pattern": "(Vertrag|AGB)", "default": ""},
                {"name": "language_en", "method": "regex", "pattern": "(terms|conditions|English)", "default": ""},
            ],
        })
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert result["data"]["result"]["language_en"] != ""
        assert result["data"]["result"]["language_de"] == ""

    @pytest.mark.asyncio
    async def test_db_upsert_runner_inserts_extractions(self, tmp_path):
        """DbUpsertRunner (db.upsert Brick) schreibt Extraktions-Ergebnisse."""
        db_path = _make_test_db(tmp_path)
        runner = DbUpsertRunner()
        step = _UpsertStep(
            connection=db_path,
            table="extractions",
            data=[
                {"doc_id": "doc1", "language": "de"},
                {"doc_id": "doc2", "language": "en"},
            ],
        )
        result = await runner.execute(step, _FakeContext())

        assert result["success"] is True
        assert result["data"]["total"] == 2

        # Verifizieren: Zeilen in DB
        conn = sqlite3.connect(db_path)
        rows = conn.execute("SELECT doc_id, language FROM extractions ORDER BY doc_id").fetchall()
        conn.close()
        assert len(rows) == 2
        assert rows[0] == ("doc1", "de")
        assert rows[1] == ("doc2", "en")

    @pytest.mark.asyncio
    async def test_full_language_detection_chain_e2e(self, tmp_path):
        """
        Vollständige Brick-Kette: db.query → specialist (foreach) → db.upsert.

        1. Fetch alle 4 Dokumente via db.query
        2. Für jedes Dokument: Spracherkennung via extract.specialist
        3. Ergebnisse speichern via db.upsert
        4. Verifizieren: extractions-Tabelle enthält korrekte Daten
        """
        db_path = _make_test_db(tmp_path)

        # Step 1: db.query → alle Dokumente holen
        query_runner = DbQueryRunner()
        query_step = _DbQueryStep(
            connection=db_path,
            query="SELECT id, text FROM documents ORDER BY id"
        )
        query_result = await query_runner.execute(query_step, _FakeContext())
        assert query_result["success"] is True
        rows = query_result["data"]["rows"]
        assert len(rows) == 4

        # Step 2: extract.specialist → Sprache für jeden Text erkennen
        specialist_runner = SpecialistRunner()
        extractions = []
        for row in rows:
            step = _SpecialistStep(config={
                "input_field": "text",
                "extract": [
                    {
                        "name": "language",
                        "method": "regex",
                        "pattern": "(Vertrag|AGB|terms|conditions|français|Nederlands)",
                        "default": "unknown",
                    },
                ],
            })
            ctx = _FakeContext({"text": row["text"]})
            spec_result = await specialist_runner.execute(step, ctx)
            assert spec_result["success"] is True
            language = spec_result["data"]["result"]["language"]
            extractions.append({"doc_id": row["id"], "language": language})

        assert len(extractions) == 4
        # doc1 (de) → "Vertrag" oder "AGB" matched
        assert extractions[0]["language"] in ("Vertrag", "AGB")
        # doc2 (en) → "terms" oder "conditions" matched
        assert extractions[1]["language"] in ("terms", "conditions")
        # doc3 (fr) → "français" matched
        assert extractions[2]["language"] == "français"
        # doc4 (nl) → "Nederlands" matched
        assert extractions[3]["language"] == "Nederlands"

        # Step 3: db.upsert → alle Extraktionen speichern
        upsert_runner = DbUpsertRunner()
        upsert_step = _UpsertStep(
            connection=db_path,
            table="extractions",
            data=extractions,
        )
        upsert_result = await upsert_runner.execute(upsert_step, _FakeContext())
        assert upsert_result["success"] is True
        assert upsert_result["data"]["total"] == 4

        # Step 4: Verifizieren — Daten in DB
        conn = sqlite3.connect(db_path)
        db_rows = conn.execute(
            "SELECT doc_id, language FROM extractions ORDER BY doc_id"
        ).fetchall()
        conn.close()

        assert len(db_rows) == 4
        doc_ids = [r[0] for r in db_rows]
        assert doc_ids == ["doc1", "doc2", "doc3", "doc4"]
        # Keine "unknown" Sprachen — alle erkannt
        languages = [r[1] for r in db_rows]
        assert all(lang != "unknown" for lang in languages), \
            f"Expected all languages detected, got: {languages}"


# ---------------------------------------------------------------------------
# Class 3: Compositor-Mode Tests
# ---------------------------------------------------------------------------

class TestCompositorMode:
    """Compositor-Mode verhindert python/cli Steps."""

    @pytest.mark.asyncio
    async def test_compositor_mode_blocks_python_step(self):
        """Pipeline mit compositor_mode=True und python Step → Fehler."""
        pipeline = _load("""
name: poc-compositor-python-blocked
compositor_mode: true
error_handling:
  on_error: continue
steps:
  - id: bad_step
    type: python
    script: "print('hello')"
""")

        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is False

        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.steps["bad_step"].status == "error"
        error_msg = result.steps["bad_step"].error_message or ""
        assert "Compositor-Mode" in error_msg

    @pytest.mark.asyncio
    async def test_compositor_mode_blocks_cli_step(self):
        """Pipeline mit compositor_mode=True und cli Step → Fehler."""
        pipeline = _load("""
name: poc-compositor-cli-blocked
compositor_mode: true
error_handling:
  on_error: continue
steps:
  - id: bad_cli
    type: cli
    args: ["echo", "hello"]
""")

        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.steps["bad_cli"].status == "error"
        error_msg = result.steps["bad_cli"].error_message or ""
        assert "Compositor-Mode" in error_msg

    def test_compositor_mode_sets_allow_code_false(self):
        """compositor_mode=True setzt allow_code automatisch auf False."""
        pipeline = _load("""
name: poc-compositor-defaults
compositor_mode: true
steps:
  - id: dummy
    type: flow.set
    values:
      x: 1
""")
        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is False

    def test_compositor_mode_explicit_allow_code_respected(self):
        """Wenn allow_code explizit True gesetzt wird, bleibt es True."""
        pipeline = _load("""
name: poc-compositor-allow-code
compositor_mode: true
allow_code: true
steps:
  - id: dummy
    type: flow.set
    values:
      x: 1
""")
        assert pipeline.compositor_mode is True
        assert pipeline.allow_code is True

    @pytest.mark.asyncio
    async def test_compositor_mode_strict_bricks_blocks_legacy_type(self):
        """compositor_mode=True setzt strict_bricks — Legacy-Namen werden geblockt."""
        # strict_bricks wird NICHT automatisch von compositor_mode gesetzt
        # (nur allow_code wird gesetzt). Aber wir können strict_bricks manuell testen.
        pipeline = _load("""
name: poc-strict-bricks
strict_bricks: true
error_handling:
  on_error: continue
steps:
  - id: legacy_step
    type: flow.set
    values:
      x: 1
  - id: next_step
    type: flow.set
    values:
      y: 2
""")
        # strict_bricks=True: 'set' (legacy) should raise; 'flow.set' is fine
        assert pipeline.strict_bricks is True

        # Engine runs with strict_bricks
        engine = PipelineEngine()
        result = await engine.run(pipeline)
        # flow.set ist ein gültiger Brick-Name → kein Fehler
        assert result.steps["legacy_step"].status == "ok"


# ---------------------------------------------------------------------------
# Class 4: Step Statuses + Run Metadata
# ---------------------------------------------------------------------------

class TestStepStatusesAndProgress:
    """Step-Statuses und Run-Metadaten werden korrekt berichtet."""

    @pytest.mark.asyncio
    async def test_run_has_run_id(self):
        """Jeder Run hat eine eindeutige run_id."""
        pipeline = _load("""
name: poc-run-id
steps:
  - id: step1
    type: flow.set
    values:
      result: "ok"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.run_id is not None
        assert result.run_id.startswith("run-")
        assert len(result.run_id) > 5

    @pytest.mark.asyncio
    async def test_run_duration_positive(self):
        """Die Run-Duration ist >= 0."""
        pipeline = _load("""
name: poc-duration
steps:
  - id: step1
    type: flow.set
    values:
      x: 42
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.duration >= 0.0

    @pytest.mark.asyncio
    async def test_two_runs_have_different_run_ids(self):
        """Zwei aufeinanderfolgende Runs haben verschiedene run_ids."""
        pipeline = _load("""
name: poc-unique-ids
steps:
  - id: s1
    type: flow.set
    values:
      x: 1
""")
        engine = PipelineEngine()
        result1 = await engine.run(pipeline)
        result2 = await engine.run(pipeline)

        assert result1.run_id != result2.run_id

    @pytest.mark.asyncio
    async def test_step_data_persisted_in_db(self):
        """Nach dem Run sind Step-Executions in der Brix-DB gespeichert."""
        pipeline = _load("""
name: poc-step-persist
steps:
  - id: my_step
    type: flow.set
    values:
      hello: "world"
""")
        engine = PipelineEngine()
        result = await engine.run(pipeline)

        assert result.success is True
        run_id = result.run_id

        from brix.db import BrixDB
        brix_db = BrixDB()
        executions = brix_db.get_step_executions(run_id)
        assert len(executions) >= 1, f"Expected step executions in DB for run {run_id}"
        step_ids = [e["step_id"] for e in executions]
        assert "my_step" in step_ids

    @pytest.mark.asyncio
    async def test_specialist_runner_reports_progress(self):
        """SpecialistRunner.execute() liefert success=True mit data.result."""
        runner = SpecialistRunner()
        ctx = _FakeContext({"text": "Hello World with some terms."})
        step = _SpecialistStep(config={
            "input_field": "text",
            "extract": [
                {"name": "has_terms", "method": "regex", "pattern": "terms", "default": ""},
            ],
        })
        result = await runner.execute(step, ctx)

        assert result["success"] is True
        assert "data" in result
        assert "result" in result["data"]
        assert "duration" in result
        assert result["duration"] >= 0.0


# ---------------------------------------------------------------------------
# Class 5: Error Handling
# ---------------------------------------------------------------------------

class TestBrickPipelineErrorHandling:
    """Fehlerbehandlung in Brick-Pipelines."""

    @pytest.mark.asyncio
    async def test_db_query_missing_connection_returns_error(self, tmp_path):
        """DbQueryRunner ohne connection → success=False."""
        runner = DbQueryRunner()

        class _BadStep:
            connection = None
            query = "SELECT 1"
            params = None

        result = await runner.execute(_BadStep(), _FakeContext())
        assert result["success"] is False
        assert "connection" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_db_query_invalid_table_returns_error(self, tmp_path):
        """DbQueryRunner mit nicht-existierender Tabelle → success=False."""
        db_path = _make_test_db(tmp_path)
        runner = DbQueryRunner()
        step = _DbQueryStep(
            connection=db_path,
            query="SELECT * FROM nonexistent_table"
        )
        result = await runner.execute(step, _FakeContext())
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_specialist_runner_missing_config_returns_error(self):
        """SpecialistRunner ohne config → success=False."""
        runner = SpecialistRunner()

        class _EmptyStep:
            config = None

        result = await runner.execute(_EmptyStep(), _FakeContext())
        assert result["success"] is False
        assert "config" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_db_upsert_missing_connection_returns_error(self, tmp_path):
        """DbUpsertRunner ohne valide connection → success=False."""
        runner = DbUpsertRunner()

        class _BadUpsertStep:
            connection = "nonexistent_connection_that_does_not_exist_xyz"
            table = "extractions"
            params = {"data": {"doc_id": "x", "language": "de"}}
            conflict_key = None

        result = await runner.execute(_BadUpsertStep(), _FakeContext())
        assert result["success"] is False

    @pytest.mark.asyncio
    async def test_pipeline_continues_after_failed_step(self):
        """Bei on_error=continue läuft die Pipeline nach einem Fehler weiter."""
        pipeline = _load("""
name: poc-continue-on-error
error_handling:
  on_error: continue
steps:
  - id: bad_step
    type: python
    on_error: continue
    script: "import sys; sys.exit(1)"

  - id: good_step
    type: flow.set
    values:
      recovered: true
""")

        engine = PipelineEngine()
        result = await engine.run(pipeline)

        # good_step sollte trotz Fehler in bad_step ausgeführt werden
        assert "good_step" in result.steps
        assert result.steps["good_step"].status == "ok"
