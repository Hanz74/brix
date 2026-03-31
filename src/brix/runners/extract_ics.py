"""Extract ICS runner — parse ICS/iCal files and extract events.

Brick name: extract.ics (T-BRIX-BRICK-03)

Uses stdlib only (no icalendar library) — parses VEVENT blocks with regex.
"""

import re
import time
from pathlib import Path
from typing import Any

from brix.runners.base import BaseRunner


def _unfold_ics(text: str) -> str:
    """Unfold ICS line continuations (RFC 5545 §3.1).

    Lines that begin with a single whitespace character are continuations
    of the previous line.
    """
    return re.sub(r"\r?\n[ \t]", "", text)


def _parse_vevent(block: str) -> dict:
    """Parse a single VEVENT block into a dict.

    Handles common properties: SUMMARY, DTSTART, DTEND, LOCATION,
    DESCRIPTION, UID, STATUS, ORGANIZER, ATTENDEE.
    """
    event: dict[str, Any] = {}

    # Simple single-value properties
    simple_props = {
        "SUMMARY": "summary",
        "DTSTART": "start",
        "DTEND": "end",
        "LOCATION": "location",
        "DESCRIPTION": "description",
        "UID": "uid",
        "STATUS": "status",
        "ORGANIZER": "organizer",
        "RRULE": "rrule",
        "DURATION": "duration",
    }

    for ics_name, dict_key in simple_props.items():
        # Match property with optional parameters (e.g. DTSTART;TZID=...:20240101T120000)
        pattern = re.compile(
            rf"^{re.escape(ics_name)}(?:;[^:]*)?:(.*)$",
            re.MULTILINE,
        )
        match = pattern.search(block)
        if match:
            value = match.group(1).strip()
            # Clean up escaped characters
            value = value.replace("\\n", "\n").replace("\\,", ",").replace("\\;", ";")
            event[dict_key] = value

    # Multi-value: ATTENDEE (can appear multiple times)
    attendees = re.findall(
        r"^ATTENDEE(?:;[^:]*)?:(.*)$",
        block,
        re.MULTILINE,
    )
    if attendees:
        event["attendees"] = [a.strip() for a in attendees]

    return event


def parse_ics(text: str, event_types: list[str] | None = None) -> list[dict]:
    """Parse an ICS string and return a list of event dicts.

    Args:
        text: Raw ICS file content.
        event_types: Optional filter — only return events matching these
                     STATUS values (case-insensitive). If None, return all.
    """
    unfolded = _unfold_ics(text)

    # Extract all VEVENT blocks
    pattern = re.compile(
        r"BEGIN:VEVENT\s*\n(.*?)END:VEVENT",
        re.DOTALL,
    )
    blocks = pattern.findall(unfolded)

    events = []
    for block in blocks:
        event = _parse_vevent(block)
        if not event:
            continue

        # Apply event_types filter if specified
        if event_types:
            status = event.get("status", "").upper()
            summary = event.get("summary", "").upper()
            # Match against status or check if any type keyword is in summary
            type_match = any(
                et.upper() == status or et.upper() in summary
                for et in event_types
            )
            if not type_match:
                continue

        events.append(event)

    return events


class ExtractIcsRunner(BaseRunner):
    """Parses an ICS/iCal file and extracts calendar events.

    Pipeline YAML example::

        - id: parse_calendar
          type: extract.ics
          params:
            path: "/host/root/calendars/team.ics"

        # With event type filter
        - id: confirmed_only
          type: extract.ics
          params:
            path: "/host/root/calendars/team.ics"
            event_types:
              - CONFIRMED
    """

    def config_schema(self) -> dict:
        return {
            "type": "object",
            "properties": {
                "path": {
                    "type": "string",
                    "description": "Path to the .ics file to parse",
                },
                "content": {
                    "type": "string",
                    "description": "Raw ICS content (alternative to path)",
                },
                "event_types": {
                    "type": "array",
                    "items": {"type": "string"},
                    "description": "Optional filter: only return events matching these types/statuses",
                },
            },
            "required": [],
        }

    def validate_config(self, config: dict) -> list[str]:
        errors = super().validate_config(config)
        path = config.get("path")
        content = config.get("content")
        if not path and not content:
            errors.append("extract.ics needs either 'path' or 'content'")
        event_types = config.get("event_types")
        if event_types is not None and not isinstance(event_types, list):
            errors.append("'event_types' must be a list of strings")
        return errors

    def input_type(self) -> str:
        return "none"

    def output_type(self) -> str:
        return "dict"

    async def execute(self, step: Any, context: Any) -> dict:
        start = time.monotonic()

        params = getattr(step, "params", {}) or {}
        path = params.get("path")
        content = params.get("content")
        event_types = params.get("event_types")

        if not path and not content:
            return {"success": False, "error": "extract.ics needs 'path' or 'content'", "duration": 0.0}

        # Read ICS content
        if content:
            ics_text = content
        else:
            try:
                file_path = Path(path)
                if not file_path.exists():
                    return {"success": False, "error": f"File not found: {path}", "duration": 0.0}
                ics_text = file_path.read_text(encoding="utf-8")
            except PermissionError:
                return {"success": False, "error": f"Permission denied: {path}", "duration": 0.0}
            except Exception as exc:
                return {"success": False, "error": str(exc), "duration": 0.0}

        self.report_progress(0.0, "Parsing ICS content")

        try:
            events = parse_ics(ics_text, event_types=event_types)
        except Exception as exc:
            return {"success": False, "error": f"ICS parse error: {exc}", "duration": time.monotonic() - start}

        duration = time.monotonic() - start
        self.report_progress(100.0, f"Extracted {len(events)} events", done=len(events), total=len(events))
        return {
            "success": True,
            "data": {
                "events": events,
                "count": len(events),
            },
            "duration": duration,
        }
