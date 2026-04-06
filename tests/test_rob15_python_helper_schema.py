from brix.runners.python import PythonRunner


def test_validate_config_with_only_helper_has_no_errors():
    runner = PythonRunner()
    assert runner.validate_config({"helper": "my_helper"}) == []


def test_validate_config_with_only_script_has_no_errors():
    runner = PythonRunner()
    assert runner.validate_config({"script": "tests/helpers/echo_params.py"}) == []


def test_validate_config_with_neither_returns_error():
    runner = PythonRunner()
    errors = runner.validate_config({})
    assert errors
    assert any("script" in err.lower() or "helper" in err.lower() for err in errors)


def test_validate_config_with_both_has_no_errors():
    runner = PythonRunner()
    assert runner.validate_config(
        {"script": "tests/helpers/echo_params.py", "helper": "my_helper"}
    ) == []
