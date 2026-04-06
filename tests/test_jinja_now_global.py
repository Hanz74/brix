import datetime
import uuid

import pytest

from brix.loader import PipelineLoader


def test_now_renders_datetime_like_string():
    loader = PipelineLoader()

    rendered = loader.render_template("{{ now() }}", {})

    assert rendered
    parsed = datetime.datetime.fromisoformat(rendered)
    assert parsed.tzinfo is not None


def test_now_strftime_renders_todays_utc_date():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ now().strftime('%Y%m%d') }}", {})

    assert str(rendered) == datetime.datetime.now(datetime.timezone.utc).strftime("%Y%m%d")


def test_now_works_in_concatenation():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ 'prefix_' ~ now().strftime('%H%M%S') }}", {})

    assert rendered.startswith("prefix_")
    assert rendered[7:].isdigit()
    assert len(rendered) == 13


def test_utcnow_alias_is_available_in_step_params_and_conditions():
    loader = PipelineLoader()
    pipeline = loader.load_from_string("""
name: t
steps:
  - id: s1
    type: http
    url: https://example.com/{{ utcnow().strftime('%Y%m%d') }}
    when: "{{ utcnow().strftime('%Y') | int >= 2000 }}"
""")
    step = pipeline.steps[0]

    rendered = loader.render_step_params(step, {})

    assert rendered["_url"].startswith("https://example.com/")
    assert rendered["_url"][-8:].isdigit()
    assert loader.evaluate_condition(step.when, {}) is True


def test_uuid4_global_returns_uuid_string():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ uuid4() }}", {})

    assert isinstance(rendered, str)
    assert len(rendered) == 36
    assert str(uuid.UUID(rendered)) == rendered


def test_fromjson_filter_parses_json_objects():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ '{}' | fromjson }}", {})

    assert rendered == {}


def test_zip_global_supports_pairwise_iteration():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ zip([1, 2], ['a', 'b']) | list }}", {})

    assert rendered == [(1, "a"), (2, "b")]


def test_env_global_reads_process_environment():
    loader = PipelineLoader()

    rendered = loader.render_value("{{ env('PATH') }}", {})

    assert isinstance(rendered, str)
    assert rendered


def test_fail_global_raises_value_error():
    loader = PipelineLoader()

    with pytest.raises(ValueError, match="boom"):
        loader.render_value("{{ fail('boom') }}", {})
