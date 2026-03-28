"""Brick type compatibility system — T-BRIX-V8-06.

Defines which output types are compatible as input for the next pipeline step,
and provides converter suggestions when direct compatibility is not possible.

T-BRIX-DB-06: TYPE_COMPATIBILITY is seeded into DB and read from there at runtime.
"""
from __future__ import annotations

import logging

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# Compatibility table
#
# Key   = output_type of step N
# Value = list of input_types that step N+1 can accept
#
# Special token "*" matches any type (wildcard).
# ---------------------------------------------------------------------------

TYPE_COMPATIBILITY: dict[str, list[str]] = {
    # No input — first step in a pipeline (produces nothing / is a source)
    "none": [],
    # Wildcard — compatible with anything (handled by special rules in is_compatible)
    "*": [],
    # Generic list variants — list[email] is a specialisation of list[dict] / list[*]
    "list[*]": ["list[*]", "list[dict]", "list[email]", "list[file_ref]", "list[object]"],
    "list[dict]": ["list[dict]", "list[*]"],
    "list[email]": ["list[email]", "list[dict]", "list[*]"],
    "list[file_ref]": ["list[file_ref]", "list[dict]", "list[*]"],
    "list[object]": ["list[object]", "list[dict]", "list[*]"],
    # Scalar types — strings are interchangeable with text / markdown variants
    "dict": ["dict", "object"],
    "object": ["object", "dict"],
    "text": ["text", "string"],
    "string": ["string", "text"],
    "string (markdown)": ["string (markdown)", "string", "text", "markdown"],
    "string (text)": ["string (text)", "string", "text"],
    "json": ["json", "dict", "object"],
    "markdown": ["markdown", "string", "text"],
    "file_path": ["file_path", "string"],
    # Compound output types
    "object|list": ["object|list", "object", "dict", "list[*]", "list[dict]"],
    "list|object": ["list|object", "list[*]", "list[dict]", "object", "dict"],
    # Parameterised output types from domain bricks — all subsets of object/dict
    "object (extracted fields)": ["object (extracted fields)", "object", "dict"],
    "object (category + rationale)": ["object (category + rationale)", "object", "dict"],
    "object (insert_count, upsert_count, errors)": [
        "object (insert_count, upsert_count, errors)", "object", "dict"
    ],
    "object (sent_at, channel, status)": [
        "object (sent_at, channel, status)", "object", "dict"
    ],
    "object (source, destination, operation, success)": [
        "object (source, destination, operation, success)", "object", "dict"
    ],
}

# ---------------------------------------------------------------------------
# Converter suggestions
#
# Maps (output_type_prefix, input_type_prefix) → converter brick name.
# Prefix matching is used so we don't need an entry per parameterised variant.
# ---------------------------------------------------------------------------

_CONVERTER_SUGGESTIONS: list[tuple[str, str, str]] = [
    # file_path → string (markdown)
    ("file_path", "string", "convert.to_markdown"),
    ("file_path", "markdown", "convert.to_markdown"),
    ("file_path", "text", "convert.extract_text"),
    # file_path → list
    ("file_path", "list", "convert.to_json"),
    ("file_path", "object", "convert.to_json"),
    ("file_path", "dict", "convert.to_json"),
    # list → string (LLM processing)
    ("list[email]", "string", "convert.to_markdown"),
    ("list[email]", "text", "convert.extract_text"),
    # object/dict → list
    ("object", "list", "transform"),
    ("dict", "list", "transform"),
    # string → list
    ("string", "list", "transform"),
    ("text", "list", "transform"),
]


def _get_type_compatibility() -> dict[str, list[str]]:
    """Return type compatibility table — from DB if available, else from code."""
    try:
        from brix.db import BrixDB
        db = BrixDB()
        if db.type_compatibility_count() > 0:
            return db.type_compatibility_as_dict()
    except Exception as e:
        logger.debug("Could not load type_compatibility from DB: %s", e)
    return TYPE_COMPATIBILITY


def _normalise(type_str: str) -> str:
    """Lowercase and strip a type string for comparison."""
    return (type_str or "").strip().lower()


def is_compatible(output_type: str, input_type: str) -> bool:
    """Return True if output_type from step N is compatible with input_type of step N+1.

    Rules (in order):
    1. Either side is empty → compatible (untyped brick — no constraint)
    2. Either side is "*" → compatible (wildcard)
    3. Exact match (case-insensitive)
    4. Table lookup: output_type → list of accepted input_types
    5. Prefix compatibility: list[X] → list[*] / list[dict]
    """
    out = _normalise(output_type)
    inp = _normalise(input_type)

    # Rule 1 — untyped bricks have no constraint
    if not out or not inp:
        return True

    # Rule 2 — wildcard
    if out == "*" or inp == "*":
        return True

    # Rule 3 — exact match
    if out == inp:
        return True

    # Rule 4 — table lookup (try exact key, then lowercased key)
    def _check_in_vals(vals: list[str]) -> bool:
        normalised_vals = [_normalise(v) for v in vals]
        return inp in normalised_vals

    compat_table = _get_type_compatibility()
    if output_type in compat_table:
        if _check_in_vals(compat_table[output_type]):
            return True
    else:
        # Case-insensitive key lookup
        for key, vals in compat_table.items():
            if _normalise(key) == out:
                if _check_in_vals(vals):
                    return True
                break

    # Rule 5 — prefix list compatibility
    # list[X] output → list[*] or list[dict] input (broadening)
    if out.startswith("list[") and inp in ("list[*]", "list[dict]", "list[object]"):
        return True
    # list[*] output → list[X] input (narrowing is OK for typed lists)
    if out in ("list[*]", "list[dict]", "list[object]") and inp.startswith("list["):
        return True

    return False


def suggest_converter(output_type: str, input_type: str) -> str | None:
    """Suggest a converter brick name when output_type → input_type is incompatible.

    Returns the brick name (e.g. 'convert.to_markdown') or None if no known
    converter can bridge the gap.
    """
    out = _normalise(output_type)
    inp = _normalise(input_type)

    for (out_prefix, inp_prefix, brick) in _CONVERTER_SUGGESTIONS:
        if out.startswith(_normalise(out_prefix)) and inp.startswith(_normalise(inp_prefix)):
            return brick

    return None
