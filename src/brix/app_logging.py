"""Structured application event logging — T-BRIX-LOG-01.

Provides a thin convenience layer over BrixDB.write_app_log() for
structured event logging from scheduler, trigger, watchdog, and
startup-sync components.

Usage:
    from brix.app_logging import log_event
    log_event("INFO", "scheduler", "Pipeline started", {"pipeline": "my-pipe"})
"""
from __future__ import annotations

import json
import logging
from typing import Any, Optional

logger = logging.getLogger(__name__)


def log_event(
    level: str,
    component: str,
    message: str,
    details: Optional[dict[str, Any]] = None,
) -> Optional[str]:
    """Write a structured event to the app_log table.

    Parameters
    ----------
    level:
        Log level: 'INFO', 'WARNING', 'ERROR'.
    component:
        Source component: 'scheduler', 'trigger', 'watchdog', 'startup_sync'.
    message:
        Human-readable event description.
    details:
        Optional dict of structured metadata. Serialized as JSON and
        appended to the message as `` | <json>``.

    Returns
    -------
    The entry ID string on success, or None if logging failed.
    """
    try:
        from brix.db import BrixDB
        db = BrixDB()
        full_message = message
        if details:
            full_message = f"{message} | {json.dumps(details, default=str)}"
        return db.write_app_log(level=level, component=component, message=full_message)
    except Exception as exc:
        # Never let logging failures crash the caller
        logger.debug("log_event failed: %s", exc)
        return None
