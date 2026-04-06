"""Keyword filter runner — filter items by keyword matching on specified fields.

Brick name: filter.keyword (T-BRIX-BRICK-03)
"""

import time
from typing import Any

from brix.runners.base import BaseRunner


class KeywordFilterRunner(BaseRunner):
    """Filters a list of dicts by keyword matching on specified fields.

    Pipeline YAML example::

        - id: relevant_emails
          type: filter.keyword
          params:
            input: "{{ fetch.output }}"
            fields:
              - subject
              - body
            keywords:
              - invoice
              - payment
            mode: any
            case_sensitive: false
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "input": {"description": "List of dicts to filter"},
                "fields": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Field names to search in each item",
                },
                "keywords": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Keywords to match against",
                },
                "mode": {
                    "type": "string",
                    "enum": ["any", "all"],
                    "description": "Match mode: 'any' (default) = at least one keyword, 'all' = all keywords",
                },
                "case_sensitive": {
                    "type": "boolean",
                    "description": "Whether matching is case-sensitive (default: false)",
                },
            },
            "required": ["fields", "keywords"],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        fields = config.get("fields")
        if fields is not None and not isinstance(fields, list):
            errors.append("'fields' must be a list of strings")
        keywords = config.get("keywords")
        if keywords is not None and not isinstance(keywords, list):
            errors.append("'keywords' must be a list of strings")
        mode = config.get("mode")
        if mode is not None and mode not in ("any", "all"):
            errors.append("'mode' must be 'any' or 'all'")
        return errors

    def input_type(self) -> str:
        return "list[dict]"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        input_data = params.get("input") if "input" in params else params.get("_input")
        fields = params.get("fields", [])
        keywords = params.get("keywords", [])
        mode = params.get("mode", "any")
        case_sensitive = params.get("case_sensitive", False)

        if input_data is None:
            return {"success": False, "error": "filter.keyword needs 'input' (a list)", "duration": 0.0}
        if not isinstance(input_data, list):
            return {"success": False, "error": f"filter.keyword input must be a list, got {type(input_data).__name__}", "duration": 0.0}
        if not fields:
            return {"success": False, "error": "filter.keyword needs 'fields' (list of field names)", "duration": 0.0}
        if not keywords:
            return {"success": False, "error": "filter.keyword needs 'keywords' (list of keywords)", "duration": 0.0}
        if mode not in ("any", "all"):
            return {"success": False, "error": f"filter.keyword 'mode' must be 'any' or 'all', got: {mode!r}", "duration": 0.0}

        # Prepare keywords for matching
        if not case_sensitive:
            match_keywords = [str(kw).lower() for kw in keywords]
        else:
            match_keywords = list(keywords)

        matching = []
        for item in input_data:
            if not isinstance(item, dict):
                continue

            # Build the searchable text from specified fields
            field_texts = []
            for field in fields:
                val = item.get(field)
                if val is not None:
                    text = str(val)
                    if not case_sensitive:
                        text = text.lower()
                    field_texts.append(text)

            combined_text = " ".join(field_texts)

            if mode == "any":
                if any(kw in combined_text for kw in match_keywords):
                    matching.append(item)
            else:  # mode == "all"
                if all(kw in combined_text for kw in match_keywords):
                    matching.append(item)

        filtered_out = len(input_data) - len(matching)
        duration = time.monotonic() - start
        self.report_progress(100.0, "done", done=len(matching), total=len(input_data))
        return {
            "success": True,
            "data": {
                "items": matching,
                "count": len(matching),
                "filtered_out": filtered_out,
            },
            "duration": duration,
        }
