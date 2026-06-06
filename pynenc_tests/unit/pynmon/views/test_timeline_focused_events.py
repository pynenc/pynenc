"""Tests for focused event markers on the invocation timeline."""

from __future__ import annotations

from datetime import UTC, datetime, timedelta

from pynenc.trigger.monitoring import EventMarkerPage, EventRecord
from pynenc_tests.conftest import MockPynenc
from pynmon.util.svg.models import TimelineConfig
from pynmon.views.invocations import TimelineRequest, _load_event_markers


def _make_app(event: EventRecord) -> MockPynenc:
    app = MockPynenc()
    app.trigger.get_event_markers_in_timerange.return_value = EventMarkerPage(
        markers=[], total=0, truncated=False
    )
    app.trigger.get_event.side_effect = (
        lambda event_id: event if event_id == event.event_id else None
    )
    app.trigger.get_trigger_runs_in_timerange.return_value = []
    app.state_backend.iter_history_in_timerange.return_value = iter([])
    return app


def test_focused_matched_event_is_loaded_when_marker_query_filters_it_out() -> None:
    timestamp = datetime(2026, 5, 18, 13, 28, 10, 527000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-result",
        event_code="result.any.captured",
        timestamp=timestamp,
        matched_condition_ids=["cond-1"],
        valid_condition_ids=["cond-1"],
        emitted_by_invocation_id="emitter-inv",
    )
    req = TimelineRequest(
        app=_make_app(event),
        start_time=timestamp - timedelta(milliseconds=500),
        end_time=timestamp + timedelta(milliseconds=750),
        config=TimelineConfig(resolution_seconds=0.1),
        limit=500,
        focus_event=event.event_id,
    )

    markers = _load_event_markers(req, trigger_runs=[])

    assert len(markers) == 1
    marker = markers[0]
    assert marker.event_id == "evt-result"
    assert marker.emitted_by_invocation_id == "emitter-inv"
    assert marker.triggered is False
    assert marker.condition_types == ["ResultContext"]


def test_focused_recorded_only_event_is_not_forced_onto_timeline() -> None:
    timestamp = datetime(2026, 5, 18, 13, 28, 10, 527000, tzinfo=UTC)
    event = EventRecord(
        event_id="evt-result",
        event_code="result.any.captured",
        timestamp=timestamp,
        emitted_by_invocation_id="emitter-inv",
    )
    req = TimelineRequest(
        app=_make_app(event),
        start_time=timestamp - timedelta(milliseconds=500),
        end_time=timestamp + timedelta(milliseconds=750),
        config=TimelineConfig(resolution_seconds=0.1),
        limit=500,
        focus_event=event.event_id,
    )

    assert _load_event_markers(req, trigger_runs=[]) == []
