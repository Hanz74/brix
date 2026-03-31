"""Extract URL runner — extract URLs from text fields.

Brick name: extract.url (T-BRIX-BRICK-03)
"""

import re
import time
from typing import Any

from brix.runners.base import BaseRunner

# Standard URL regex — matches http(s), ftp, and common URL patterns
_DEFAULT_URL_PATTERN = (
    r"https?://(?:[-\w.]|(?:%[\da-fA-F]{2}))+"
    r"(?:/[-\w%.~:/?#\[\]@!$&'()*+,;=]*)*"
)


class ExtractUrlRunner(BaseRunner):
    """Extracts URLs from text fields in input items.

    Pipeline YAML examples::

        # Extract from a list of items
        - id: find_links
          type: extract.url
          params:
            input: "{{ fetch.output }}"
            field: body

        # Extract from a single dict
        - id: find_links_single
          type: extract.url
          params:
            input: "{{ prev.output }}"
            field: content

        # With custom regex
        - id: find_custom
          type: extract.url
          params:
            input: "{{ fetch.output }}"
            field: text
            pattern: "https://example\\.com/[\\w/]+"
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "List of dicts or single dict to extract URLs from"},
                "field": {
                    "type": "string",
                    "description": "Field name to extract URLs from",
                },
                "pattern": {
                    "type": "string",
                    "description": "Custom regex pattern for URL matching (optional, default: standard URL regex)",
                },
            },
            "required": ["field"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        field = config.get("field")
        if field is not None and not isinstance(field, str):
            errors.append("'field' must be a string")
        pattern = config.get("pattern")
        if pattern is not None:
            try:
                re.compile(pattern)
            except re.error as exc:
                errors.append(f"'pattern' is not a valid regex: {exc}")
        return errors

    def input_type(self) -> str:
        return "list[dict] | dict"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        field = params.get("field")
        pattern = params.get("pattern", _DEFAULT_URL_PATTERN)

        if input_data is None:
            return {"success": False, "error": "extract.url needs 'input'", "duration": 0.0}
        if not field:
            return {"success": False, "error": "extract.url needs 'field'", "duration": 0.0}

        try:
            compiled = re.compile(pattern)
        except re.error as exc:
            return {"success": False, "error": f"Invalid regex pattern: {exc}", "duration": 0.0}

        # Normalise to list
        if isinstance(input_data, dict):
            items = [input_data]
        elif isinstance(input_data, list):
            items = input_data
        else:
            return {"success": False, "error": f"extract.url input must be a dict or list, got {type(input_data).__name__}", "duration": 0.0}

        all_urls: list[str] = []
        for item in items:
            if not isinstance(item, dict):
                continue
            text = item.get(field)
            if text is None:
                continue
            text = str(text)
            found = compiled.findall(text)
            all_urls.extend(found)

        # Deduplicate while preserving order
        seen: set[str] = set()
        unique_urls: list[str] = []
        for url in all_urls:
            if url not in seen:
                seen.add(url)
                unique_urls.append(url)

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Extracted {len(unique_urls)} URLs", done=len(unique_urls), total=len(items))
        return {
            "success": True,
            "data": {
                "urls": unique_urls,
                "count": len(unique_urls),
            },
            "duration": duration,
        }
