from __future__ import annotations

import pytest

from brix.db import BrixDB
from brix.mcp_handlers.helpers import _handle_create_helper, _handle_get_helper


@pytest.mark.asyncio
async def test_get_helper_response_shows_db_code_presence(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper_db = BrixDB(db_path=tmp_path / "helpers.db")
    monkeypatch.setattr("brix.helper_registry.BrixDB", lambda: helper_db)
    monkeypatch.setattr("brix.db.BrixDB", lambda *args, **kwargs: helper_db)

    code = (
        "import json\n"
        "def main():\n"
        "    print(json.dumps({'ok': True, 'source': 'db'}))\n"
        "\n"
        "if __name__ == '__main__':\n"
        "    main()\n"
    )

    create_result = await _handle_create_helper(
        {
            "name": "db_visible_helper",
            "description": "Helper stored in DB for MCP response testing.",
            "input_schema": {},
            "output_schema": {},
            "code": code,
        }
    )
    assert create_result["success"] is True

    result = await _handle_get_helper({"name": "db_visible_helper"})

    assert result["success"] is True
    helper = result["helper"]
    assert helper["source"] == "db"
    assert helper["has_code"] is True
    assert helper["code_length"] == len(code)
    assert helper["code_preview"] == code[:200]
    assert helper["code_preview"].startswith("import json")
    assert helper["legacy_script_path"] == ""
    assert "script" not in helper
