"""Helpers for precise Pynmon timeline zoom windows."""

from __future__ import annotations

import logging
from collections.abc import Sequence
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

from pynenc.identifiers.invocation_id import InvocationId

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.monitoring import TriggerRunRecord

logger = logging.getLogger(__name__)


def calculate_timeline_zoom_window(
    timestamps: Sequence[datetime | None],
) -> tuple[datetime, datetime] | None:
    """Return the same padded window used by the timeline zoom button."""
    times = sorted(
        {
            normalized.isoformat(): normalized
            for timestamp in timestamps
            if timestamp is not None
            for normalized in [_normalize_to_millisecond(timestamp)]
        }.values()
    )
    if not times:
        return None

    start_time = times[0]
    end_time = times[-1]
    duration_ms = max((end_time - start_time).total_seconds() * 1000, 0)
    padding_ms = 100 if duration_ms == 0 else min(max(duration_ms * 0.15, 50), 750)
    padding = timedelta(milliseconds=padding_ms)
    return start_time - padding, end_time + padding


def trigger_run_zoom_timestamps(run: TriggerRunRecord) -> list[datetime | None]:
    """Collect trigger context timestamps used when zooming its invocation."""
    timestamps: list[datetime | None] = [run.claimed_at, run.executed_at]
    timestamps.extend(participant.context_timestamp for participant in run.participants)
    return timestamps


def invocation_history_timestamps(
    app: Pynenc, invocation_id: str | InvocationId
) -> list[datetime | None]:
    """Return status history timestamps for an invocation, or an empty list."""
    try:
        return [
            entry.timestamp
            for entry in app.state_backend.get_history(InvocationId(str(invocation_id)))
        ]
    except Exception:
        logger.debug("state backend history lookup failed for %s", invocation_id)
        return []


def invocation_trigger_run(
    app: Pynenc, invocation_id: str | InvocationId
) -> TriggerRunRecord | None:
    """Return the trigger run that produced *invocation_id*, if one exists."""
    try:
        runs = app.trigger.get_trigger_runs_for_invocation(str(invocation_id))
    except Exception:
        logger.debug("trigger backend lookup failed for invocation %s", invocation_id)
        return None
    return runs[0] if runs else None


def invocation_zoom_window(
    app: Pynenc,
    invocation_id: str | InvocationId,
    trigger_run: TriggerRunRecord | None = None,
    extra_timestamps: Sequence[datetime | None] | None = None,
) -> tuple[datetime, datetime] | None:
    """Calculate the URL window for zooming to an invocation and its trigger."""
    timestamps: list[datetime | None] = invocation_history_timestamps(
        app, invocation_id
    )
    if trigger_run is not None:
        timestamps.extend(trigger_run_zoom_timestamps(trigger_run))
    if extra_timestamps:
        timestamps.extend(extra_timestamps)
    return calculate_timeline_zoom_window(timestamps)


def _normalize_to_millisecond(timestamp: datetime) -> datetime:
    """Match browser Date precision before calculating URL bounds."""
    if timestamp.tzinfo is None:
        timestamp = timestamp.replace(tzinfo=UTC)
    else:
        timestamp = timestamp.astimezone(UTC)
    return timestamp.replace(microsecond=(timestamp.microsecond // 1000) * 1000)
