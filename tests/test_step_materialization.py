from __future__ import annotations

from brix.materialize import materialize_step
from brix.models import Step
from brix.validator import StepAnalysis
from brix.loader import PipelineLoader
from brix.engine import _RenderedStep


def test_materialize_step_promotes_config_fields_with_provenance() -> None:
    step = Step(
        id="save_results",
        type="db.exec",
        connection="wrong-db",
        config={
            "connection": "buddy-db",
            "query": "UPDATE documents SET raw_structured = $1::jsonb WHERE id = $2::int",
        },
    )

    materialized = materialize_step(step)

    assert materialized.effective_config["connection"] == "buddy-db"
    assert materialized.promoted_fields["connection"]["source"] == "config.connection"
    assert materialized.promoted_fields["connection"]["raw_top_level"] == "wrong-db"
    assert materialized.promoted_fields["connection"]["effective_value"] == "buddy-db"


def test_materialize_step_preserves_list_params_shape() -> None:
    step = Step(
        id="insert_user",
        type="db.exec",
        params=["{{ input.name }}", "{{ input.age }}"],
        config={"connection": "buddy-db", "query": "INSERT INTO users (name, age) VALUES ($1, $2)"},
    )

    materialized = materialize_step(step)

    assert materialized.raw_params == ["{{ input.name }}", "{{ input.age }}"]
    assert materialized.effective_params == ["{{ input.name }}", "{{ input.age }}"]


def test_materialize_step_resolves_legacy_aliases() -> None:
    step = Step(
        id="legacy-pipeline",
        type="pipeline",
        pipeline="child-pipeline",
    )

    materialized = materialize_step(step)

    assert materialized.raw_type == "pipeline"
    assert materialized.effective_type == "flow.pipeline"
    assert materialized.policy_flags["uses_legacy_alias"] is True


def test_materialize_step_exposes_reserved_wrapper_keys() -> None:
    step = Step(id="noop", type="flow.set")

    materialized = materialize_step(step)

    assert "_config" in materialized.wrapper_keys
    assert "_params" in materialized.wrapper_keys
    assert "_pipeline" in materialized.wrapper_keys


def test_materialize_step_tracks_conditional_refs() -> None:
    step = Step(
        id="conditional-extract",
        type="flow.pipeline",
        pipeline="extract-child",
        when="{{ input.enabled }}",
        foreach="{{ source.output.items }}",
        depends_on=["source"],
    )

    materialized = materialize_step(step)

    assert materialized.dependency_refs["when"] == "{{ input.enabled }}"
    assert materialized.dependency_refs["foreach"] == "{{ source.output.items }}"
    assert materialized.dependency_refs["depends_on"] == ("source",)
    assert materialized.policy_flags["uses_conditional_refs"] is True


def test_materialize_step_exposes_provenance() -> None:
    step = Step(id="legacy-pipeline", type="pipeline", pipeline="child-pipeline")

    materialized = materialize_step(step)

    assert materialized.provenance["persisted_from"] == "step-model"
    assert "merge_step_config_into_params" in materialized.provenance["normalizers"]
    assert "materialize_step" in materialized.provenance["normalizers"]


def test_rendered_step_uses_materialized_top_level_promotions() -> None:
    loader = PipelineLoader()
    step = Step(
        id="subpipe",
        type="flow.pipeline",
        config={"pipeline": "child-pipeline"},
    )

    rendered_step = _RenderedStep(step, loader.render_step_params(step, {}), loader, {})

    assert rendered_step.pipeline == "child-pipeline"


def test_step_analysis_uses_materialized_effective_shape() -> None:
    step = Step(
        id="query",
        type="db.query",
        config={"connection": "buddy-db", "query": "SELECT * FROM documents", "params": {"limit": 1}},
    )

    analysis = StepAnalysis.from_step(step)

    assert analysis.normalized_config == {
        "connection": "buddy-db",
        "query": "SELECT * FROM documents",
        "params": {"limit": 1},
    }
    assert analysis.normalized_params == {"limit": 1}
    assert analysis.materialized.dependency_refs["connection"] == "buddy-db"
