"""Brix Alerting — AlertRule management and post-run alert evaluation.

Stores alert rules and alert history in brix.db (BrixDB).

Supported conditions:
  - "pipeline_failed"              — any pipeline failure
  - "pipeline_failed_consecutive:N" — N consecutive failures for a pipeline
  - "run_hung"                     — run >X minutes without heartbeat
  - "dependency_missing"           — helper requirement not installed
  - "monthly_cost_exceeds:N"       — monthly LLM cost (USD) exceeds threshold N
  - "mcp_server_down:N"            — MCP server has not been contacted for >N minutes

Supported channels:
  - "log"        — write to alert_history + stderr
  - "mattermost" — POST JSON to webhook_url in config
"""
from __future__ import annotations

import json
import sys
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Optional
from uuid import uuid4

from brix.config import config


def _now_iso() -> str:
    return datetime.now(timezone.utc).isoformat()


# ---------------------------------------------------------------------------
# AlertRule dataclass
# ---------------------------------------------------------------------------

@dataclass
class AlertRule:
    id: str
    name: str
    condition: str
    channel: str
    config: dict
    enabled: bool
    created_at: str


# ---------------------------------------------------------------------------
# AlertManager
# ---------------------------------------------------------------------------

VALID_CONDITIONS = {
    "pipeline_failed",
    "run_hung",
    "dependency_missing",
    "monthly_cost_exceeds",
    "mcp_server_down",
    "step_regression",
}
VALID_CHANNELS = {"log", "mattermost"}


class AlertManager:
    """Manages alert rules and fires alerts after pipeline runs.

    Parameters
    ----------
    db:
        A :class:`brix.db.BrixDB` instance. If not provided, a new one is
        created using the default database path.
    db_path:
        Deprecated — kept for backward compatibility. Ignored when *db* is
        provided. If only *db_path* is supplied a BrixDB is created for that
        path.
    """

    def __init__(
        self,
        db: Optional[Any] = None,
        db_path: Optional[Path] = None,
    ) -> None:
        if db is not None:
            self._db = db
        else:
            from brix.db import BrixDB, BRIX_DB_PATH
            path = Path(db_path) if db_path else BRIX_DB_PATH
            path.parent.mkdir(parents=True, exist_ok=True)
            self._db = BrixDB(db_path=path)
        # db_path kept for compatibility (e.g. _check_monthly_cost_exceeds)
        self.db_path = self._db.db_path

    def _connect(self):
        """Expose BrixDB connection for backward-compatibility (e.g. tests)."""
        return self._db._connect()

    # ------------------------------------------------------------------
    # Rule CRUD (delegates to BrixDB)
    # ------------------------------------------------------------------

    def add_rule(
        self,
        name: str,
        condition: str,
        channel: str,
        config: Optional[dict] = None,
    ) -> AlertRule:
        """Add a new alert rule. Returns the created AlertRule."""
        self._validate_condition(condition)
        self._validate_channel(channel)
        row = self._db.alert_rule_add(
            name=name,
            condition=condition,
            channel=channel,
            config=config or {},
        )
        return self._dict_to_rule(row)

    def list_rules(self) -> list[AlertRule]:
        """Return all alert rules."""
        return [self._dict_to_rule(r) for r in self._db.alert_rule_list()]

    def get_rule(self, rule_id: str) -> Optional[AlertRule]:
        """Return a rule by ID, or None if not found."""
        row = self._db.alert_rule_get(rule_id)
        return self._dict_to_rule(row) if row else None

    def update_rule(
        self,
        rule_id: str,
        name: Optional[str] = None,
        condition: Optional[str] = None,
        channel: Optional[str] = None,
        config: Optional[dict] = None,
        enabled: Optional[bool] = None,
    ) -> Optional[AlertRule]:
        """Update an existing rule. Returns the updated rule or None if not found."""
        if condition is not None:
            self._validate_condition(condition)
        if channel is not None:
            self._validate_channel(channel)
        row = self._db.alert_rule_update(
            rule_id=rule_id,
            name=name,
            condition=condition,
            channel=channel,
            config=config,
            enabled=enabled,
        )
        return self._dict_to_rule(row) if row else None

    def delete_rule(self, rule_id: str) -> bool:
        """Delete a rule by ID. Returns True if deleted."""
        return self._db.alert_rule_delete(rule_id)

    # ------------------------------------------------------------------
    # Alert History (delegates to BrixDB)
    # ------------------------------------------------------------------

    def get_alert_history(self, limit: int = 20) -> list[dict]:
        """Return the most recent alert history entries, newest first."""
        return self._db.alert_history_list(limit=limit)

    def _record_alert(
        self,
        rule: AlertRule,
        message: str,
        pipeline: Optional[str] = None,
        run_id: Optional[str] = None,
    ) -> None:
        """Persist an alert firing to history."""
        self._db.alert_history_add(
            rule_id=rule.id,
            rule_name=rule.name,
            condition=rule.condition,
            channel=rule.channel,
            message=message,
            pipeline=pipeline,
            run_id=run_id,
        )

    # ------------------------------------------------------------------
    # Alert evaluation
    # ------------------------------------------------------------------

    def check_alerts(self, run_result: Any) -> list[dict]:
        """Check all enabled rules against a completed run result.

        Parameters
        ----------
        run_result:
            A :class:`brix.models.RunResult` or any object with ``success``,
            ``run_id``, and optionally ``pipeline`` attributes/keys.

        Returns a list of fired alert records (dicts).
        """
        rules = [r for r in self.list_rules() if r.enabled]
        if not rules:
            return []

        # Normalise run_result to dict-like access
        success = _get_attr(run_result, "success", True)
        run_id = _get_attr(run_result, "run_id", None)
        pipeline_name = _get_attr(run_result, "pipeline", None)

        fired: list[dict] = []
        for rule in rules:
            if self._matches(rule, run_result, success, pipeline_name):
                msg = self._build_message(rule, run_result, pipeline_name)
                self._record_alert(rule, msg, pipeline=pipeline_name, run_id=run_id)
                self._dispatch(rule, msg, pipeline_name, run_id)
                fired.append({
                    "rule_id": rule.id,
                    "rule_name": rule.name,
                    "condition": rule.condition,
                    "channel": rule.channel,
                    "message": msg,
                    "pipeline": pipeline_name,
                    "run_id": run_id,
                })
        return fired

    def _matches(
        self,
        rule: AlertRule,
        run_result: Any,
        success: bool,
        pipeline_name: Optional[str],
    ) -> bool:
        """Return True if *run_result* matches *rule.condition*."""
        cond = rule.condition

        if cond == "pipeline_failed":
            return not success

        if cond.startswith("pipeline_failed_consecutive:"):
            if success:
                return False
            try:
                n = int(cond.split(":", 1)[1])
            except (IndexError, ValueError):
                return False
            return self._count_consecutive_failures(pipeline_name) >= n

        if cond == "run_hung":
            # Check based on a configurable max_minutes threshold in rule.config
            # In practice this is checked from the history by an external scheduler.
            # At run-completion time we can only detect if the run was marked as
            # failed/timed-out — we conservatively return False here (the scheduler
            # is responsible for heartbeat checks).
            return False

        if cond == "dependency_missing":
            return self._check_dependency_missing(run_result)

        if cond.startswith("monthly_cost_exceeds:"):
            try:
                threshold = float(cond.split(":", 1)[1])
            except (IndexError, ValueError):
                return False
            return self._check_monthly_cost_exceeds(threshold)

        if cond.startswith("mcp_server_down:"):
            try:
                max_minutes = float(cond.split(":", 1)[1])
            except (IndexError, ValueError):
                return False
            server_name = rule.config.get("server_name")
            return self._check_mcp_server_down(server_name, max_minutes)

        if cond == "step_regression":
            return self._check_step_regression(run_result, pipeline_name, rule)

        return False

    def _count_consecutive_failures(self, pipeline_name: Optional[str]) -> int:
        """Count how many of the most-recent runs for *pipeline_name* failed consecutively."""
        if not pipeline_name:
            return 0
        with self._db._connect() as conn:
            conn.row_factory = __import__("sqlite3").Row
            rows = conn.execute(
                """SELECT success FROM runs
                   WHERE pipeline=?
                   ORDER BY started_at DESC
                   LIMIT 20""",
                (pipeline_name,),
            ).fetchall()
        count = 0
        for row in rows:
            if row["success"] == 0:
                count += 1
            else:
                break
        return count

    def _check_dependency_missing(self, run_result: Any) -> bool:
        """Return True if run_result indicates a missing dependency."""
        # Check steps for error messages mentioning missing packages
        steps = _get_attr(run_result, "steps", {}) or {}
        for step_data in steps.values():
            err = ""
            if hasattr(step_data, "error_message"):
                err = step_data.error_message or ""
            elif isinstance(step_data, dict):
                err = step_data.get("error_message", "") or ""
            if "requirement" in err.lower() or "install" in err.lower() or "missing" in err.lower():
                return True
        return False

    def _check_monthly_cost_exceeds(self, threshold: float) -> bool:
        """Return True if total LLM cost for the current calendar month exceeds *threshold* USD."""
        try:
            monthly_cost = self._db.get_monthly_cost_usd()
            return monthly_cost > threshold
        except Exception:
            return False

    def _check_step_regression(
        self,
        run_result: Any,
        pipeline_name: Optional[str],
        rule: "AlertRule",
    ) -> bool:
        """Return True if any step's avg_duration exceeds 3× the median of the last 10 runs.

        Reads step duration history from the runs table via BrixDB.
        The rule.config may optionally specify:
          - "step_id": only check this specific step
          - "multiplier": regression threshold (default 3.0)
          - "history_runs": how many past runs to consider (default 10)
        """
        if not pipeline_name:
            return False

        multiplier = float(rule.config.get("multiplier", 3.0))
        history_runs = int(rule.config.get("history_runs", 10))
        target_step_id = rule.config.get("step_id")

        # Get the steps from the current run result
        steps = _get_attr(run_result, "steps", {}) or {}
        if not steps:
            return False

        for step_id, step_data in steps.items():
            # Filter to target step if configured
            if target_step_id and step_id != target_step_id:
                continue

            # Get current duration
            if hasattr(step_data, "duration"):
                current_dur = step_data.duration
            elif isinstance(step_data, dict):
                current_dur = step_data.get("duration")
            else:
                continue

            if current_dur is None:
                continue

            try:
                current_dur = float(current_dur)
            except (TypeError, ValueError):
                continue

            # Get historical durations
            past_durations = self._db.get_step_durations(
                pipeline=pipeline_name,
                step_id=step_id,
                limit=history_runs,
            )

            if len(past_durations) < 2:
                # Not enough history to detect regression
                continue

            # Compute median of historical durations
            sorted_durs = sorted(past_durations)
            n = len(sorted_durs)
            if n % 2 == 1:
                median_dur = sorted_durs[n // 2]
            else:
                median_dur = (sorted_durs[n // 2 - 1] + sorted_durs[n // 2]) / 2.0

            if median_dur <= 0.0:
                continue

            if current_dur > multiplier * median_dur:
                return True

        return False

    def _check_mcp_server_down(
        self, server_name: Optional[str], max_minutes: float
    ) -> bool:
        """Return True if *server_name* has not been contacted for more than *max_minutes*.

        Reads health data from the active ``McpConnectionPool`` instance if
        available.  If no pool is active (no running pipeline), the server is
        considered unreachable and the condition fires only when *server_name*
        is explicitly configured and has previously had contact recorded.

        Args:
            server_name: Server to check.  If ``None``, checks *all* servers
                in the pool — fires if ANY server exceeds the threshold.
            max_minutes: How many minutes of silence constitute "down".
        """
        try:
            from brix.context import _active_pool  # type: ignore[attr-defined]
            if _active_pool is None:
                return False
            health = _active_pool.get_health()
        except (ImportError, AttributeError):
            return False

        if not health:
            return False

        max_seconds = max_minutes * 60.0
        now = datetime.now(timezone.utc)

        def _is_server_down(name: str, stats: dict) -> bool:
            last_contact_str = stats.get("last_contact_at", "")
            if not last_contact_str:
                return True
            try:
                last_contact = datetime.fromisoformat(last_contact_str)
                age_seconds = (now - last_contact).total_seconds()
                return age_seconds > max_seconds
            except (ValueError, TypeError):
                return False

        if server_name:
            if server_name not in health:
                # Never contacted — not necessarily "down" yet
                return False
            return _is_server_down(server_name, health[server_name])

        # No specific server — fire if ANY server is down
        return any(_is_server_down(name, stats) for name, stats in health.items())

    def _build_message(
        self,
        rule: AlertRule,
        run_result: Any,
        pipeline_name: Optional[str],
    ) -> str:
        run_id = _get_attr(run_result, "run_id", "unknown")
        return (
            f"[Brix Alert] Rule '{rule.name}' fired: condition='{rule.condition}' "
            f"pipeline='{pipeline_name}' run_id='{run_id}'"
        )

    def _dispatch(
        self,
        rule: AlertRule,
        message: str,
        pipeline_name: Optional[str],
        run_id: Optional[str],
    ) -> None:
        """Send the alert via the configured channel."""
        if rule.channel == "log":
            print(f"BRIX ALERT: {message}", file=sys.stderr)

        elif rule.channel == "mattermost":
            webhook_url = rule.config.get("webhook_url")
            if not webhook_url:
                print(
                    f"[Brix Alert] Mattermost channel missing webhook_url in rule '{rule.name}'",
                    file=sys.stderr,
                )
                return
            self._send_mattermost(webhook_url, message)

    def _send_mattermost(self, webhook_url: str, message: str) -> None:
        """POST a Mattermost webhook."""
        try:
            import urllib.request

            payload = json.dumps({"text": message}).encode()
            req = urllib.request.Request(
                webhook_url,
                data=payload,
                headers={"Content-Type": "application/json"},
                method="POST",
            )
            with urllib.request.urlopen(req, timeout=config.MATTERMOST_WEBHOOK_TIMEOUT):
                pass
        except Exception as exc:
            print(f"[Brix Alert] Mattermost webhook failed: {exc}", file=sys.stderr)

    # ------------------------------------------------------------------
    # Internal helpers
    # ------------------------------------------------------------------

    @staticmethod
    def _dict_to_rule(row: dict) -> AlertRule:
        return AlertRule(
            id=row["id"],
            name=row["name"],
            condition=row["condition"],
            channel=row["channel"],
            config=row.get("config") if isinstance(row.get("config"), dict) else json.loads(row.get("config") or "{}"),
            enabled=bool(row.get("enabled", 1)),
            created_at=row["created_at"],
        )

    @staticmethod
    def _validate_condition(condition: str) -> None:
        base = condition.split(":")[0]
        valid_bases = {
            "pipeline_failed",
            "pipeline_failed_consecutive",
            "run_hung",
            "dependency_missing",
            "monthly_cost_exceeds",
            "mcp_server_down",
            "step_regression",
        }
        if base not in valid_bases:
            raise ValueError(
                f"Unknown alert condition: '{condition}'. "
                f"Valid: pipeline_failed, pipeline_failed_consecutive:N, run_hung, "
                f"dependency_missing, monthly_cost_exceeds:N, mcp_server_down:N, "
                f"step_regression"
            )

    @staticmethod
    def _validate_channel(channel: str) -> None:
        if channel not in VALID_CHANNELS:
            raise ValueError(
                f"Unknown alert channel: '{channel}'. Valid: {', '.join(sorted(VALID_CHANNELS))}"
            )


# ---------------------------------------------------------------------------
# Utility
# ---------------------------------------------------------------------------

def _get_attr(obj: Any, attr: str, default: Any = None) -> Any:
    """Get attribute from object or dict."""
    if isinstance(obj, dict):
        return obj.get(attr, default)
    return getattr(obj, attr, default)
