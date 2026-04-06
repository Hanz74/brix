from pathlib import Path
from types import SimpleNamespace
from unittest import mock

from brix.loader import PipelineLoader
from brix.runners.pipeline import PipelineRunner
from brix.runners.pipeline_group import PipelineGroupRunner


def _pipeline(name: str):
    return PipelineLoader().load_from_string(
        f"""
name: {name}
steps:
  - id: noop
    type: set
    key: value
    value: ok
"""
    )


async def test_sub_pipeline_loaded_from_db_not_disk():
    db_pipeline = _pipeline("sub-db")
    engine = SimpleNamespace(
        loader=mock.Mock(),
        run=mock.AsyncMock(return_value=SimpleNamespace(success=True, result={"source": "db"})),
    )
    engine.loader.load.side_effect = AssertionError("disk loader should not be used")

    runner = PipelineRunner(engine=engine)
    step = SimpleNamespace(pipeline="sub-db", params={})
    context = SimpleNamespace(_pipeline_depth=0)

    with mock.patch("brix.runners.pipeline.PipelineStore.load", return_value=db_pipeline) as store_load:
        result = await runner.execute(step, context)

    assert result["success"] is True
    assert result["data"] == {"source": "db"}
    store_load.assert_called_once_with("sub-db")
    engine.loader.load.assert_not_called()
    engine.run.assert_awaited_once_with(db_pipeline, {})


async def test_sub_pipeline_falls_back_to_disk_when_not_in_db(tmp_path, monkeypatch):
    home_dir = tmp_path / "home"
    system_dir = home_dir / ".brix" / "pipelines" / "_system"
    system_dir.mkdir(parents=True)
    disk_path = system_dir / "sys-sub.yaml"
    disk_path.write_text(
        """
name: sys-sub
steps:
  - id: noop
    type: set
    key: value
    value: ok
"""
    )

    disk_pipeline = _pipeline("sys-sub")
    engine = SimpleNamespace(
        loader=mock.Mock(),
        run=mock.AsyncMock(return_value=SimpleNamespace(success=True, result={"source": "disk"})),
    )
    engine.loader.load.return_value = disk_pipeline

    runner = PipelineRunner(engine=engine)
    step = SimpleNamespace(pipeline="_system/sys-sub", params={})
    context = SimpleNamespace(_pipeline_depth=0)

    monkeypatch.setattr("brix.runners.pipeline.Path.home", lambda: home_dir)

    with mock.patch(
        "brix.runners.pipeline.PipelineStore.load",
        side_effect=FileNotFoundError("not in db"),
    ) as store_load:
        result = await runner.execute(step, context)

    assert result["success"] is True
    assert result["data"] == {"source": "disk"}
    store_load.assert_called_once_with("_system/sys-sub")
    engine.loader.load.assert_called_once_with(str(disk_path))
    engine.run.assert_awaited_once_with(disk_pipeline, {})


async def test_pipeline_group_loads_three_sub_pipelines_from_db():
    pipelines = {name: _pipeline(name) for name in ("alpha", "beta", "gamma")}

    async def fake_run(pipeline, params):
        return SimpleNamespace(success=True, result={"name": pipeline.name, "params": params})

    engine = SimpleNamespace(
        loader=mock.Mock(),
        run=mock.AsyncMock(side_effect=fake_run),
    )
    engine.loader.load.side_effect = AssertionError("disk loader should not be used")

    runner = PipelineGroupRunner(engine=engine)
    step = SimpleNamespace(
        pipelines=["alpha", "beta", "gamma"],
        shared_params={"batch": "42"},
        concurrency=3,
    )
    context = SimpleNamespace(to_jinja_context=lambda: {})

    with mock.patch(
        "brix.runners.pipeline_group.PipelineStore.load",
        side_effect=lambda name: pipelines[name],
    ) as store_load:
        result = await runner.execute(step, context)

    assert result["success"] is True
    assert result["data"]["total"] == 3
    assert result["data"]["succeeded"] == 3
    assert result["data"]["failed"] == 0
    assert result["data"]["errors"] == {}
    assert result["data"]["results"] == {
        "alpha": {"name": "alpha", "params": {"batch": "42"}},
        "beta": {"name": "beta", "params": {"batch": "42"}},
        "gamma": {"name": "gamma", "params": {"batch": "42"}},
    }
    assert store_load.call_args_list == [mock.call("alpha"), mock.call("beta"), mock.call("gamma")]
    engine.loader.load.assert_not_called()
