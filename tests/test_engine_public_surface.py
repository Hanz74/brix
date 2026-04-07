def test_import_pipeline_engine():
    from brix.engine import PipelineEngine

    assert PipelineEngine is not None


def test_import_legacy_aliases():
    from brix.engine import LEGACY_ALIASES

    assert isinstance(LEGACY_ALIASES, dict)


def test_import_rendered_step():
    from brix.engine import _RenderedStep

    assert _RenderedStep is not None


def test_import_validate_config_top_level_fields():
    from brix.engine import _VALIDATE_CONFIG_TOP_LEVEL_FIELDS

    assert isinstance(_VALIDATE_CONFIG_TOP_LEVEL_FIELDS, tuple)


def test_import_step_config_dict():
    from brix.engine import _step_config_dict

    assert callable(_step_config_dict)


def test_import_extract_brick_default_values():
    from brix.engine import _extract_brick_default_values

    assert callable(_extract_brick_default_values)


def test_import_extract_step_cost():
    from brix.engine import _extract_step_cost

    assert callable(_extract_step_cost)


def test_import_redact_secret_values():
    from brix.engine import _redact_secret_values

    assert callable(_redact_secret_values)


def test_import_measure_rss_mb():
    from brix.engine import _measure_rss_mb

    assert callable(_measure_rss_mb)


def test_import_total_ram_mb():
    from brix.engine import _total_ram_mb

    assert callable(_total_ram_mb)


def test_import_warn_if_high_memory():
    from brix.engine import _warn_if_high_memory

    assert callable(_warn_if_high_memory)
