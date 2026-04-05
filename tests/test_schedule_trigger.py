"""Tests for the schedule/cron trigger runner (T-BRIX-BUG-19)."""
import pytest
from datetime import datetime, timezone

from brix.triggers.runners import (
    _parse_cron_field,
    cron_matches,
    ScheduleTriggerRunner,
    TRIGGER_RUNNERS,
)
from brix.triggers.models import TriggerConfig
from brix.triggers.state import TriggerState


# ---------------------------------------------------------------------------
# cron field parsing
# ---------------------------------------------------------------------------


class TestParseCronField:
    def test_star(self):
        assert _parse_cron_field("*", 0, 59) == set(range(0, 60))

    def test_exact(self):
        assert _parse_cron_field("5", 0, 59) == {5}

    def test_step(self):
        # */3 in hour field (0-23) -> {0, 3, 6, 9, 12, 15, 18, 21}
        result = _parse_cron_field("*/3", 0, 23)
        assert result == {0, 3, 6, 9, 12, 15, 18, 21}

    def test_range(self):
        assert _parse_cron_field("1-5", 0, 59) == {1, 2, 3, 4, 5}

    def test_range_with_step(self):
        assert _parse_cron_field("0-10/3", 0, 59) == {0, 3, 6, 9}

    def test_comma_separated(self):
        assert _parse_cron_field("1,5,10", 0, 59) == {1, 5, 10}

    def test_comma_mixed(self):
        # "1,10-12,*/20"
        result = _parse_cron_field("1,10-12,*/20", 0, 59)
        assert 1 in result
        assert {10, 11, 12}.issubset(result)
        assert {0, 20, 40}.issubset(result)


# ---------------------------------------------------------------------------
# cron_matches
# ---------------------------------------------------------------------------


class TestCronMatches:
    def test_every_3_hours_at_minute_0(self):
        expr = "0 */3 * * *"
        # 2026-03-31 06:00 UTC should match
        dt = datetime(2026, 3, 31, 6, 0, tzinfo=timezone.utc)
        assert cron_matches(expr, dt) is True

    def test_every_3_hours_not_matching(self):
        expr = "0 */3 * * *"
        # 06:15 -> minute 15 != 0
        dt = datetime(2026, 3, 31, 6, 15, tzinfo=timezone.utc)
        assert cron_matches(expr, dt) is False
        # 07:00 -> hour 7 not in {0,3,6,9,...}
        dt2 = datetime(2026, 3, 31, 7, 0, tzinfo=timezone.utc)
        assert cron_matches(expr, dt2) is False

    def test_specific_time(self):
        expr = "30 14 * * *"  # every day at 14:30
        dt_match = datetime(2026, 3, 31, 14, 30, tzinfo=timezone.utc)
        dt_no = datetime(2026, 3, 31, 14, 31, tzinfo=timezone.utc)
        assert cron_matches(expr, dt_match) is True
        assert cron_matches(expr, dt_no) is False

    def test_day_of_week_monday(self):
        # "0 9 * * 1" = Mon at 09:00
        expr = "0 9 * * 1"
        # 2026-03-30 is a Monday
        dt = datetime(2026, 3, 30, 9, 0, tzinfo=timezone.utc)
        assert cron_matches(expr, dt) is True
        # 2026-03-31 is a Tuesday
        dt2 = datetime(2026, 3, 31, 9, 0, tzinfo=timezone.utc)
        assert cron_matches(expr, dt2) is False

    def test_sunday_as_0_and_7(self):
        # Both 0 and 7 mean Sunday
        # 2026-03-29 is a Sunday
        dt = datetime(2026, 3, 29, 0, 0, tzinfo=timezone.utc)
        assert cron_matches("0 0 * * 0", dt) is True
        assert cron_matches("0 0 * * 7", dt) is True

    def test_specific_month(self):
        expr = "0 0 1 6 *"  # midnight on June 1st
        dt_june = datetime(2026, 6, 1, 0, 0, tzinfo=timezone.utc)
        dt_march = datetime(2026, 3, 1, 0, 0, tzinfo=timezone.utc)
        assert cron_matches(expr, dt_june) is True
        assert cron_matches(expr, dt_march) is False

    def test_invalid_field_count(self):
        with pytest.raises(ValueError, match="5 fields"):
            cron_matches("* * *", datetime.now(timezone.utc))


# ---------------------------------------------------------------------------
# ScheduleTriggerRunner
# ---------------------------------------------------------------------------


class TestScheduleTriggerRunner:
    def _make_trigger(self, cron_expr: str, trigger_id: str = "sched-1") -> TriggerConfig:
        return TriggerConfig(
            id=trigger_id,
            type="schedule",
            pipeline="test-pipeline",
            cron=cron_expr,
        )

    @pytest.mark.asyncio
    async def test_fires_when_due(self, tmp_path, monkeypatch):
        """Trigger should fire when current time matches the cron expression."""
        trigger = self._make_trigger("* * * * *")  # every minute
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        events = await runner.poll()
        assert len(events) == 1
        assert events[0]["type"] == "schedule"
        assert events[0]["cron"] == "* * * * *"

    @pytest.mark.asyncio
    async def test_does_not_fire_when_not_due(self, tmp_path, monkeypatch):
        """Trigger should not fire when cron doesn't match current time."""
        # Use a cron that only matches minute 59 of hour 23 on Dec 31
        trigger = self._make_trigger("59 23 31 12 *")
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        # Patch datetime to a time that won't match
        from unittest.mock import patch
        fake_now = datetime(2026, 6, 15, 10, 30, tzinfo=timezone.utc)
        with patch("brix.triggers.runners.datetime") as mock_dt:
            mock_dt.now.return_value = fake_now
            mock_dt.fromtimestamp = datetime.fromtimestamp
            mock_dt.side_effect = lambda *a, **kw: datetime(*a, **kw)
            events = await runner.poll()

        assert len(events) == 0

    @pytest.mark.asyncio
    async def test_no_double_fire_same_minute(self, tmp_path):
        """Trigger should not fire twice within the same minute."""
        trigger = self._make_trigger("* * * * *")
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        events1 = await runner.poll()
        assert len(events1) == 1

        # Second poll in same minute should return empty
        events2 = await runner.poll()
        assert len(events2) == 0

    @pytest.mark.asyncio
    async def test_tracks_last_fired(self, tmp_path):
        """After firing, last_check should be set in state."""
        trigger = self._make_trigger("* * * * *")
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        assert state.get_last_check("sched-1") is None

        await runner.poll()

        last = state.get_last_check("sched-1")
        assert last is not None
        assert isinstance(last, float)

    @pytest.mark.asyncio
    async def test_missing_cron_returns_empty(self, tmp_path):
        """If no cron is configured, poll returns empty list."""
        trigger = TriggerConfig(
            id="sched-no-cron",
            type="schedule",
            pipeline="test-pipeline",
        )
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        events = await runner.poll()
        assert events == []

    @pytest.mark.asyncio
    async def test_cron_from_filter_dict(self, tmp_path):
        """Cron can also be passed via the filter dict."""
        trigger = TriggerConfig(
            id="sched-filter",
            type="schedule",
            pipeline="test-pipeline",
            filter={"cron": "* * * * *"},
        )
        state = TriggerState(db_path=tmp_path / "test.db")
        runner = ScheduleTriggerRunner(trigger, state)

        events = await runner.poll()
        assert len(events) == 1


# ---------------------------------------------------------------------------
# Registry
# ---------------------------------------------------------------------------


class TestRegistry:
    def test_schedule_registered(self):
        assert "schedule" in TRIGGER_RUNNERS
        assert TRIGGER_RUNNERS["schedule"] is ScheduleTriggerRunner

    def test_event_alias(self):
        assert "event" in TRIGGER_RUNNERS
        assert TRIGGER_RUNNERS["event"] is ScheduleTriggerRunner
