from __future__ import annotations

from unittest.mock import patch

import pytest

from brix.runners.db_query import DbQueryRunner


class _Step:
    def __init__(self, **kwargs):
        for key, value in kwargs.items():
            setattr(self, key, value)


@pytest.mark.asyncio
async def test_execute_reads_connection_and_query_from_config_when_params_are_bind_values():
    runner = DbQueryRunner()
    step = _Step(
        config={
            "connection": "analytics",
            "query": "SELECT * FROM users WHERE id = :id",
        },
        params={"id": 1},
    )

    with patch.object(runner, "_resolve_connection", return_value=("sqlite", ":memory:")) as resolve:
        with patch.object(runner, "_run_query", return_value=[{"id": 1}]) as run_query:
            result = await runner.execute(step, context=None)

    assert result["success"] is True
    resolve.assert_called_once_with("analytics", None)
    run_query.assert_called_once_with(
        "sqlite",
        ":memory:",
        "SELECT * FROM users WHERE id = :id",
        {"id": 1},
    )
