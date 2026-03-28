"""Resilience patterns for Brix steps (T-BRIX-DB-21).

Implements:
  - Circuit Breaker: tracks failures per brick; trips into cooldown after max_failures
  - Rate Limiter: sliding-window call rate limiting per brick
  - Brick Cache: TTL-based cache keyed by a Jinja2-rendered key
  - Saga Tracker: records compensating steps; runs them in reverse on failure

All state is persisted in the Brix SQLite DB so patterns survive process restarts.
"""

from __future__ import annotations

import hashlib
import json
import re
from datetime import datetime, timedelta, timezone
from typing import Any, Optional

from brix.db import BrixDB

# ---------------------------------------------------------------------------
# Duration parsing
# ---------------------------------------------------------------------------

_DURATION_RE = re.compile(r"^(\d+(?:\.\d+)?)\s*([smhd]?)$", re.IGNORECASE)

_UNIT_SECONDS: dict[str, float] = {
    "s": 1.0,
    "m": 60.0,
    "h": 3600.0,
    "d": 86400.0,
    "": 1.0,  # bare number → seconds
}


def parse_duration(s: str) -> float:
    """Parse a duration string like '10m', '1h', '30s' into seconds.

    Raises ValueError if the string cannot be parsed.
    """
    if isinstance(s, (int, float)):
        return float(s)
    m = _DURATION_RE.match(str(s).strip())
    if not m:
        raise ValueError(f"Cannot parse duration: {s!r}")
    value = float(m.group(1))
    unit = m.group(2).lower()
    return value * _UNIT_SECONDS[unit]


def _now_utc() -> datetime:
    return datetime.now(timezone.utc)


def _now_iso() -> str:
    return _now_utc().isoformat()


# ---------------------------------------------------------------------------
# Circuit Breaker
# ---------------------------------------------------------------------------

_CB_SKIP = "circuit_breaker_skip"
_CB_FALLBACK = "circuit_breaker_fallback"


class CircuitBreaker:
    """Per-brick circuit breaker backed by the Brix DB.

    Config dict keys (all optional with defaults):
        max_failures: int = 3          — failures before tripping
        cooldown: str = "10m"          — how long to stay open
        fallback: str | None = None    — step_id whose output to return when open
    """

    def __init__(self, brick_name: str, config: dict, db: Any) -> None:
        self.brick_name = brick_name
        self.max_failures: int = int(config.get("max_failures", 3))
        self.cooldown_seconds: float = parse_duration(str(config.get("cooldown", "10m")))
        self.fallback: Optional[str] = config.get("fallback")
        self._db = db

    # ------------------------------------------------------------------
    # Pre-execute: check if in cooldown
    # ------------------------------------------------------------------

    def pre_check(self, context: Any) -> Optional[dict]:
        """Check circuit state before executing a step.

        Returns:
          None                — proceed normally
          {"skip": True, ...} — step is in cooldown (open circuit), skip or fallback
        """
        state = self._db.cb_get(self.brick_name)
        if not state:
            return None  # No state yet → proceed

        cooldown_until = state.get("cooldown_until")
        if not cooldown_until:
            return None  # Not in cooldown

        try:
            cooldown_until_dt = datetime.fromisoformat(cooldown_until)
        except Exception:
            return None

        if _now_utc() < cooldown_until_dt:
            # Circuit is OPEN — skip or fallback
            remaining = (cooldown_until_dt - _now_utc()).total_seconds()
            fallback_data = None
            if self.fallback and context is not None:
                fallback_data = context.get_output(self.fallback)
            return {
                "success": True if self.fallback else False,
                "data": fallback_data,
                "error": None if self.fallback else f"Circuit breaker OPEN for '{self.brick_name}' ({remaining:.0f}s remaining)",
                "_cb_state": _CB_FALLBACK if self.fallback else _CB_SKIP,
            }

        # Cooldown has passed — circuit is half-open, allow the attempt
        return None

    # ------------------------------------------------------------------
    # Post-execute: update state based on result
    # ------------------------------------------------------------------

    def on_success(self) -> None:
        """Reset failure count on success (close the circuit)."""
        self._db.cb_reset(self.brick_name)

    def on_failure(self) -> None:
        """Increment failure count; trip circuit if max_failures reached."""
        state = self._db.cb_get(self.brick_name)
        current = state["failure_count"] if state else 0
        new_count = current + 1
        cooldown_until = None
        if new_count >= self.max_failures:
            cooldown_until = (_now_utc() + timedelta(seconds=self.cooldown_seconds)).isoformat()
        self._db.cb_upsert(
            brick_name=self.brick_name,
            failure_count=new_count,
            last_failure=_now_iso(),
            cooldown_until=cooldown_until,
        )


# ---------------------------------------------------------------------------
# Rate Limiter
# ---------------------------------------------------------------------------


class RateLimiter:
    """Sliding-window rate limiter backed by the Brix DB.

    Config dict keys (all optional with defaults):
        max_calls: int = 60   — maximum calls allowed in the window
        per: str = "1m"       — window duration
    """

    def __init__(self, brick_name: str, config: dict, db: Any) -> None:
        self.brick_name = brick_name
        self.max_calls: int = int(config.get("max_calls", 60))
        self.window_seconds: float = parse_duration(str(config.get("per", "1m")))
        self._db = db

    def _prune(self, timestamps: list[str]) -> list[str]:
        """Remove timestamps outside the current window."""
        cutoff = _now_utc() - timedelta(seconds=self.window_seconds)
        pruned = []
        for ts in timestamps:
            try:
                dt = datetime.fromisoformat(ts)
                if dt.tzinfo is None:
                    dt = dt.replace(tzinfo=timezone.utc)
                if dt >= cutoff:
                    pruned.append(ts)
            except Exception:
                pass
        return pruned

    def wait_seconds(self) -> float:
        """Return how many seconds to wait before the next call is allowed (0 = proceed)."""
        timestamps = self._db.rl_get_timestamps(self.brick_name)
        timestamps = self._prune(timestamps)
        if len(timestamps) < self.max_calls:
            return 0.0
        # Window is full — find when the oldest call leaves the window
        oldest_ts = min(timestamps)
        try:
            oldest_dt = datetime.fromisoformat(oldest_ts)
            if oldest_dt.tzinfo is None:
                oldest_dt = oldest_dt.replace(tzinfo=timezone.utc)
        except Exception:
            return 0.0
        wait = self.window_seconds - (_now_utc() - oldest_dt).total_seconds()
        return max(0.0, wait)

    def record_call(self) -> None:
        """Record a call timestamp (called after successful execution)."""
        timestamps = self._db.rl_get_timestamps(self.brick_name)
        timestamps = self._prune(timestamps)
        timestamps.append(_now_iso())
        self._db.rl_set_timestamps(self.brick_name, timestamps)


# ---------------------------------------------------------------------------
# Brick Cache
# ---------------------------------------------------------------------------


def _make_cache_key(key_expr: str, output_data: Any = None) -> str:
    """Build a stable cache key from the rendered expression."""
    raw = f"brix:bcache:{key_expr}"
    return hashlib.sha256(raw.encode()).hexdigest()


class BrickCache:
    """TTL-based cache for step outputs backed by the Brix DB.

    Config dict keys:
        key: str  — Jinja2 expression that resolves to the cache key
        ttl: str  — TTL duration, e.g. "1h", "30m"
    """

    def __init__(self, config: dict, db: Any) -> None:
        self.key_expr: str = config.get("key", "")
        self.ttl_seconds: float = parse_duration(str(config.get("ttl", "1h")))
        self._db = db

    def get(self, rendered_key: str) -> Optional[Any]:
        """Return cached data if present and not expired, else None."""
        ck = _make_cache_key(rendered_key)
        return self._db.bcache_get(ck)

    def set(self, rendered_key: str, data: Any) -> None:
        """Store data under the rendered key with the configured TTL."""
        ck = _make_cache_key(rendered_key)
        expires_at = (_now_utc() + timedelta(seconds=self.ttl_seconds)).isoformat()
        self._db.bcache_set(ck, data, expires_at)


# ---------------------------------------------------------------------------
# Saga Tracker
# ---------------------------------------------------------------------------


class SagaTracker:
    """Tracks completed steps that have compensating actions.

    When a step fails and saga compensation is needed, the tracker
    iterates in reverse order over all recorded steps and executes
    their compensate dicts as synthetic pipeline steps.
    """

    def __init__(self) -> None:
        # List of (step_id, compensate_dict) in execution order
        self._completed: list[tuple[str, dict]] = []

    def record(self, step_id: str, compensate: dict) -> None:
        """Record a successfully completed step with a compensate definition."""
        self._completed.append((step_id, compensate))

    async def run_compensations(self, context: Any, engine: Any, pipeline: Any) -> None:
        """Execute compensating steps in reverse order.

        Compensation errors are logged but do not raise — we do best-effort rollback.
        """
        import sys
        from brix.models import Step

        for step_id, compensate_dict in reversed(self._completed):
            comp_step_id = f"compensate_{step_id}"
            try:
                # Build a Step from the compensate dict
                comp_dict = dict(compensate_dict)
                comp_dict.setdefault("id", comp_step_id)
                comp_step = Step(**comp_dict)
                runner = engine._resolve_runner(comp_step.type)
                if runner is None:
                    print(
                        f"[Saga] No runner for compensate step '{comp_step_id}' (type={comp_step.type})",
                        file=sys.stderr,
                    )
                    continue
                from brix.engine import _RenderedStep
                jinja_ctx = context.to_jinja_context()
                rendered_params = engine.loader.render_step_params(comp_step, jinja_ctx)
                rendered_step = _RenderedStep(comp_step, rendered_params, engine.loader, jinja_ctx)
                result = await runner.execute(rendered_step, context)
                if not result.get("success"):
                    print(
                        f"[Saga] Compensation '{comp_step_id}' failed: {result.get('error')}",
                        file=sys.stderr,
                    )
            except Exception as exc:
                print(
                    f"[Saga] Compensation '{comp_step_id}' raised: {exc}",
                    file=sys.stderr,
                )
