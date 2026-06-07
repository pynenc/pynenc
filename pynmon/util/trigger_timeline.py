"""Timeline URL helpers for trigger-run references."""

from __future__ import annotations

from collections.abc import Sequence
from datetime import datetime
from typing import TYPE_CHECKING
from urllib.parse import urlencode

from pynmon.util.timeline_zoom import (
    calculate_timeline_zoom_window,
    invocation_zoom_window,
    trigger_run_zoom_timestamps,
)
from pynenc.identifiers.invocation_id import InvocationId

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.monitoring import TriggerRunRecord


def timeline_url_for_trigger_run(
    trigger_run: TriggerRunRecord,
    event_timestamps: Sequence[datetime] | None = None,
    app: Pynenc | None = None,
) -> str:
    """Return a timeline URL scoped around a trigger run.

    The URL carries the same query contract as the timeline view itself:
    a custom time window, focused event when available, selected triggered
    invocation, and an invocation scope containing the trigger inputs/outputs.
    """
    nearby_events = _event_timestamps_near_trigger_run(
        trigger_run, event_timestamps or []
    )
    window = None
    if app is not None and trigger_run.triggered_invocation_id:
        window = invocation_zoom_window(
            app,
            InvocationId(trigger_run.triggered_invocation_id),
            trigger_run,
            nearby_events,
        )
    if window is None:
        window = calculate_timeline_zoom_window(
            [*trigger_run_zoom_timestamps(trigger_run), *nearby_events]
        )
    if window is None:
        return "/invocations/timeline"

    start_time, end_time = window
    params: dict[str, str] = {
        "time_range": "custom",
        "start_date": start_time.replace(tzinfo=None).isoformat(
            timespec="microseconds"
        ),
        "end_date": end_time.replace(tzinfo=None).isoformat(timespec="microseconds"),
        "resolution": "100ms",
    }
    scoped_ids = _trigger_run_invocation_scope(trigger_run)
    if trigger_run.triggered_invocation_id:
        params["selected"] = trigger_run.triggered_invocation_id
    if scoped_ids:
        params["inv_ids"] = ",".join(scoped_ids)
    if trigger_run.event_ids:
        params["focus_event"] = trigger_run.event_ids[0]
    return "/invocations/timeline?" + urlencode(params)


def _trigger_run_invocation_scope(trigger_run: TriggerRunRecord) -> list[str]:
    """Return ordered invocation IDs that participate in a trigger run."""
    scoped_ids: list[str] = []

    def add_invocation(invocation_id: str | None) -> None:
        if invocation_id and invocation_id not in scoped_ids:
            scoped_ids.append(invocation_id)

    add_invocation(trigger_run.triggered_invocation_id)
    for source_id in trigger_run.source_invocation_ids or []:
        add_invocation(source_id)
    for participant in trigger_run.participants or []:
        add_invocation(participant.source_invocation_id)
    return scoped_ids


def _event_timestamps_near_trigger_run(
    trigger_run: TriggerRunRecord, event_timestamps: Sequence[datetime]
) -> list[datetime]:
    """Keep legacy event-only trigger records from widening the window too far."""
    run_times = [
        timestamp for timestamp in trigger_run_zoom_timestamps(trigger_run) if timestamp
    ]
    if not run_times:
        return list(event_timestamps)

    base_min = min(run_times)
    base_max = max(run_times)
    return [
        timestamp
        for timestamp in event_timestamps
        if (base_min - timestamp).total_seconds() <= 60
        and (timestamp - base_max).total_seconds() <= 60
    ]
