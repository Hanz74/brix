from brix.db import merge_step_config_into_params


def test_config_method_overrides_top_level_default_method():
    step = {
        "id": "fetch",
        "type": "http.request",
        "config": {"method": "POST", "url": "https://example.com"},
        "params": None,
        "method": "GET",
        "url": None,
    }

    result = merge_step_config_into_params(step)

    assert result["method"] == "POST"


def test_config_helper_overrides_top_level_helper():
    step = {
        "id": "run_helper",
        "type": "script.python",
        "config": {"helper": "parse_invoice"},
        "params": None,
        "helper": None,
    }

    result = merge_step_config_into_params(step)

    assert result["helper"] == "parse_invoice"


def test_config_pipeline_overrides_top_level_pipeline():
    step = {
        "id": "subpipe",
        "type": "flow.pipeline",
        "config": {"pipeline": "child-pipeline"},
        "params": None,
        "pipeline": None,
    }

    result = merge_step_config_into_params(step)

    assert result["pipeline"] == "child-pipeline"
