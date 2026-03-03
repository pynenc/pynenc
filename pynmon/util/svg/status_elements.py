"""
Status element models for timeline visualization.

Immutable value objects representing visual elements: points, segments,
connecting lines, and legacy bars.
"""

from dataclasses import dataclass
from datetime import datetime

# Truncation length for IDs (7 chars like Git short SHA)
_TRUNCATE_LENGTH = 7


@dataclass(frozen=True)
class InvocationBar:
    """Legacy bar representation (superseded by StatusPoint + StatusSegment)."""

    invocation_id: str
    start_time: datetime
    end_time: datetime
    status: str
    color: str
    tooltip: str = ""

    @property
    def duration_seconds(self) -> float:
        """Duration of this segment in seconds."""
        return (self.end_time - self.start_time).total_seconds()


@dataclass(frozen=True)
class StatusPoint:
    """
    Circle representing an instantaneous status event.

    :param str invocation_id: Invocation identifier
    :param datetime timestamp: When this status occurred
    :param str status: The invocation status
    :param str color: Hex color for this point
    :param str tooltip: Hover text
    :param int order: Sequence order within the invocation
    :param int sub_lane: Vertical sub-lane for overlapping invocations
    """

    invocation_id: str
    timestamp: datetime
    status: str
    color: str
    tooltip: str = ""
    order: int = 0
    sub_lane: int = 0


@dataclass(frozen=True)
class StatusSegment:
    """
    Bar representing a status that occupies worker time.

    Segment statuses (RUNNING, PENDING, RESUMED) show as bars.

    :param str invocation_id: Invocation identifier
    :param datetime start_time: When this status began
    :param datetime end_time: When the segment ends (next status or now)
    :param str status: The invocation status during this segment
    :param str color: Hex color (may use next status color for outcomes)
    :param str tooltip: Hover text
    :param str | None next_status: Status that ended this segment
    :param int sub_lane: Vertical sub-lane for overlapping invocations
    :param bool is_ongoing: True if segment extends to timeline end
    """

    invocation_id: str
    start_time: datetime
    end_time: datetime
    status: str
    color: str
    tooltip: str = ""
    next_status: str | None = None
    sub_lane: int = 0
    is_ongoing: bool = False

    @property
    def duration_seconds(self) -> float:
        """Duration of this segment in seconds."""
        return (self.end_time - self.start_time).total_seconds()


@dataclass(frozen=True)
class StatusLine:
    """
    Line connecting consecutive status points across lanes.

    :param str invocation_id: Invocation identifier
    :param datetime start_time: Start point timestamp
    :param datetime end_time: End point timestamp
    :param str from_status: Origin status (determines line color)
    :param str to_status: Destination status
    :param str color: Hex color (from origin status)
    :param str from_runner_id: Runner ID where the line starts
    :param str to_runner_id: Runner ID where the line ends
    :param int from_sub_lane: Sub-lane index at the start point
    :param int to_sub_lane: Sub-lane index at the end point
    """

    invocation_id: str
    start_time: datetime
    end_time: datetime
    from_status: str
    to_status: str
    color: str
    from_runner_id: str = ""
    to_runner_id: str = ""
    from_sub_lane: int = 0
    to_sub_lane: int = 0
