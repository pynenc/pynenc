"""Unit tests for the trigger run \"Show in Timeline\" URL builder.

The URL should:
1. Use the same tight window as timeline "Zoom to Invocation".
2. Pass ``inv_ids`` so the timeline scopes the lane data to the trigger
   run's invocations (triggered + sources) and does not load thousands
   of unrelated invocations.
3. Keep the ``selected`` and ``focus_event`` params for highlighting.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from urllib.parse import parse_qs, urlparse

import pytest

from pynenc.trigger.monitoring import TriggerRunRecord
from pynmon.views.trigger_runs import _timeline_url_for_run


def _run(**kwargs: object) -> TriggerRunRecord:
    defaults: dict = {
        "trigger_run_id": "run-1",
        "trigger_id": "trig-1",
        "task_id_key": "pkg.task.fn",
        "logic_value": "and",
    }
    defaults.update(kwargs)
    return TriggerRunRecord(**defaults)  # type: ignore[arg-type]


def test_url_uses_narrow_window_around_run_timestamps() -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    run = _run(
        claimed_at=now,
        executed_at=now + timedelta(milliseconds=200),
        triggered_invocation_id="trg",
    )

    url = _timeline_url_for_run(run, [])

    params = parse_qs(urlparse(url).query)
    start = datetime.fromisoformat(params["start_date"][0]).replace(tzinfo=UTC)
    end = datetime.fromisoformat(params["end_date"][0]).replace(tzinfo=UTC)
    # Same bounded padding as the in-timeline Zoom to Invocation button.
    assert (now - start).total_seconds() == pytest.approx(0.05, abs=0.001)
    assert (end - (now + timedelta(milliseconds=200))).total_seconds() == pytest.approx(
        0.05, abs=0.001
    )
    assert params["selected"] == ["trg"]
    assert params["resolution"] == ["100ms"]


def test_url_includes_inv_ids_for_triggered_and_sources() -> None:
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    run = _run(
        claimed_at=now,
        executed_at=now,
        triggered_invocation_id="trg",
        source_invocation_ids=["src-a", "src-b"],
    )

    url = _timeline_url_for_run(run, [])
    params = parse_qs(urlparse(url).query)

    inv_ids = params["inv_ids"][0].split(",")
    assert inv_ids == ["trg", "src-a", "src-b"]


def test_url_drops_old_events_outside_one_minute_window() -> None:
    """Old events must not pull the window backwards by hours."""
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    very_old = now - timedelta(hours=6)
    run = _run(
        claimed_at=now,
        executed_at=now,
        triggered_invocation_id="trg",
    )

    url = _timeline_url_for_run(run, [very_old])
    params = parse_qs(urlparse(url).query)
    start = datetime.fromisoformat(params["start_date"][0]).replace(tzinfo=UTC)

    # The 6h-old event must not have pulled the start back to 06:00.
    assert (now - start).total_seconds() < 30


def test_url_keeps_recent_events_within_one_minute_window() -> None:
    """Events that are within ~1 min of the run still extend the window."""
    now = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    recent = now - timedelta(seconds=10)
    run = _run(claimed_at=now, executed_at=now, triggered_invocation_id="trg")

    url = _timeline_url_for_run(run, [recent])
    params = parse_qs(urlparse(url).query)
    start = datetime.fromisoformat(params["start_date"][0]).replace(tzinfo=UTC)

    assert (recent - start).total_seconds() == pytest.approx(0.75, abs=0.001)


def test_url_without_any_timestamps_falls_back_to_default() -> None:
    run = _run()
    assert _timeline_url_for_run(run, []) == "/invocations/timeline"
