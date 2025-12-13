"""
Visual elements for timeline visualization.

Factory functions for creating StatusPoint, StatusSegment, and StatusLine
with auto-generated tooltips and colors.
"""

from dataclasses import dataclass
from datetime import datetime

from pynmon.util.status_colors import (
    DEFAULT_STATUS_COLOR,
    OUTCOME_STATUSES,
    STATUS_COLORS,
)
from pynmon.util.svg.models import StatusLine, StatusPoint, StatusSegment
from pynmon.util.svg.runner_info import RunnerInfo
from pynmon.util.svg.tooltips import format_segment_tooltip, format_status_point_tooltip


def create_status_point(
    invocation_id: str,
    task_id: str,
    timestamp: datetime,
    status: str,
    runner_info: RunnerInfo,
    order: int = 0,
    sub_lane: int = 0,
) -> StatusPoint:
    """Create a StatusPoint with auto-generated tooltip and color."""
    color = STATUS_COLORS.get(status, DEFAULT_STATUS_COLOR)
    tooltip = format_status_point_tooltip(invocation_id, status, timestamp, runner_info)
    return StatusPoint(
        invocation_id=invocation_id,
        task_id=task_id,
        timestamp=timestamp,
        status=status,
        color=color,
        tooltip=tooltip,
        order=order,
        sub_lane=sub_lane,
    )


@dataclass
class SegmentParams:
    """Parameters for creating a status segment."""

    invocation_id: str
    task_id: str
    start_time: datetime
    end_time: datetime
    status: str
    next_status: str | None = None
    sub_lane: int = 0
    is_ongoing: bool = False


def create_status_segment(params: SegmentParams) -> StatusSegment:
    """Create a StatusSegment with auto-generated tooltip and color."""
    color = _determine_segment_color(params.status, params.next_status)
    duration = (params.end_time - params.start_time).total_seconds()
    tooltip = format_segment_tooltip(
        params.invocation_id,
        params.status,
        duration,
        params.is_ongoing,
    )
    return StatusSegment(
        invocation_id=params.invocation_id,
        task_id=params.task_id,
        start_time=params.start_time,
        end_time=params.end_time,
        status=params.status,
        color=color,
        tooltip=tooltip,
        next_status=params.next_status,
        sub_lane=params.sub_lane,
        is_ongoing=params.is_ongoing,
    )


def _determine_segment_color(status: str, next_status: str | None) -> str:
    """Determine segment color, using outcome color if applicable."""
    if next_status and next_status in OUTCOME_STATUSES:
        return STATUS_COLORS.get(next_status, DEFAULT_STATUS_COLOR)
    return STATUS_COLORS.get(status, DEFAULT_STATUS_COLOR)


def create_status_line(
    invocation_id: str,
    start_time: datetime,
    end_time: datetime,
    from_status: str,
    to_status: str,
    from_runner_id: str,
    to_runner_id: str,
    from_sub_lane: int = 0,
    to_sub_lane: int = 0,
) -> StatusLine:
    """Create a StatusLine with auto-determined color."""
    color = STATUS_COLORS.get(from_status, DEFAULT_STATUS_COLOR)
    return StatusLine(
        invocation_id=invocation_id,
        start_time=start_time,
        end_time=end_time,
        from_status=from_status,
        to_status=to_status,
        color=color,
        from_runner_id=from_runner_id,
        to_runner_id=to_runner_id,
        from_sub_lane=from_sub_lane,
        to_sub_lane=to_sub_lane,
    )
