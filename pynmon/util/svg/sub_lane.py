"""
Sub-lane computation for timeline visualization.

This module handles the assignment of vertical sub-lanes to invocations
to avoid visual overlap when multiple invocations occur on the same runner.

The algorithm distinguishes between:
- Segment statuses (PENDING, RUNNING, etc.): Occupy space for a duration
- Point statuses (REGISTERED, SUCCESS, etc.): Instantaneous events

Key rules:
- Segments require exclusive space for their entire duration
- Points only need space at their specific timestamp
- Points can share a sub-lane if they don't overlap temporally
- Different invocations on the same runner get different sub-lanes only if they overlap
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple

from pynmon.util.status_colors import is_segment_status


class TimeInterval(NamedTuple):
    """A time interval with start and end."""

    start: datetime
    end: datetime

    def overlaps(self, other: "TimeInterval") -> bool:
        """Check if this interval overlaps with another."""
        return not (self.end <= other.start or self.start >= other.end)


@dataclass
class RunnerEntry:
    """A single status entry for an invocation on a specific runner."""

    timestamp: datetime
    status: str
    is_segment: bool = field(init=False)

    def __post_init__(self) -> None:
        self.is_segment = is_segment_status(self.status.upper())


@dataclass
class InvocationOnRunner:
    """
    Represents an invocation's presence on a specific runner.

    An invocation may span multiple runners (e.g., REGISTERED on ExternalRunner,
    then RUNNING on ThreadRunner), so we track each runner separately.
    """

    invocation_id: str
    runner_id: str
    entries: list[RunnerEntry] = field(default_factory=list)

    def add_entry(self, timestamp: datetime, status: str) -> None:
        """Add a status entry."""
        self.entries.append(RunnerEntry(timestamp=timestamp, status=status))

    @property
    def sorted_entries(self) -> list[RunnerEntry]:
        """Get entries sorted by timestamp."""
        return sorted(self.entries, key=lambda e: e.timestamp)

    def get_segment_intervals(self, end_time: datetime) -> list[TimeInterval]:
        """
        Get time intervals where this invocation has segment statuses.

        Segments occupy exclusive space for their duration.
        """
        intervals: list[TimeInterval] = []
        sorted_entries = self.sorted_entries

        for i, entry in enumerate(sorted_entries):
            if not entry.is_segment:
                continue

            # Segment ends at next entry or end_time
            if i + 1 < len(sorted_entries):
                segment_end = sorted_entries[i + 1].timestamp
            else:
                # Check if final status
                status_upper = entry.status.upper()
                terminal = {
                    "SUCCESS",
                    "FAILED",
                    "KILLED",
                    "CONCURRENCY_CONTROLLED_FINAL",
                }
                segment_end = entry.timestamp if status_upper in terminal else end_time

            intervals.append(TimeInterval(start=entry.timestamp, end=segment_end))

        return intervals

    def get_point_timestamps(self) -> list[datetime]:
        """
        Get timestamps of point-only statuses.

        Points are instantaneous and can share space if not overlapping.
        """
        return [e.timestamp for e in self.entries if not e.is_segment]


@dataclass
class SubLaneOccupancy:
    """Tracks what's occupying a sub-lane."""

    segment_intervals: list[TimeInterval] = field(default_factory=list)
    point_timestamps: list[datetime] = field(default_factory=list)

    def can_accommodate(
        self,
        segments: list[TimeInterval],
        points: list[datetime],
        point_buffer_seconds: float = 2.0,
    ) -> bool:
        """
        Check if this sub-lane can accommodate the given segments and points.

        :param segments: Segment intervals to check
        :param points: Point timestamps to check
        :param point_buffer_seconds: Minimum seconds between points to avoid overlap
        :return: True if there's no conflict
        """
        # Check segment conflicts
        for seg in segments:
            # Against existing segments
            for existing_seg in self.segment_intervals:
                if seg.overlaps(existing_seg):
                    return False
            # Against existing points (segment would cover them)
            for pt in self.point_timestamps:
                if seg.start <= pt <= seg.end:
                    return False

        # Check point conflicts
        for pt in points:
            # Against existing segments
            for existing_seg in self.segment_intervals:
                if existing_seg.start <= pt <= existing_seg.end:
                    return False
            # Against existing points (need buffer)
            for existing_pt in self.point_timestamps:
                if abs((pt - existing_pt).total_seconds()) < point_buffer_seconds:
                    return False

        return True

    def add(self, segments: list[TimeInterval], points: list[datetime]) -> None:
        """Add segments and points to this sub-lane's occupancy."""
        self.segment_intervals.extend(segments)
        self.point_timestamps.extend(points)


class SubLaneKey(NamedTuple):
    """Key for sub-lane lookup: (invocation_id, runner_id)."""

    invocation_id: str
    runner_id: str


def compute_sub_lanes(
    invocations_by_runner: dict[str, list[InvocationOnRunner]],
    end_time: datetime,
    point_buffer_seconds: float = 2.0,
) -> dict[SubLaneKey, int]:
    """
    Compute sub-lane assignments for all invocations.

    Uses an interval scheduling algorithm:
    1. For each runner, sort invocations by start time
    2. For each invocation, find the first sub-lane where it fits
    3. "Fits" means no segment overlap and no point collision (within buffer)

    :param invocations_by_runner: Invocations grouped by runner_id
    :param end_time: End time for open segments
    :param point_buffer_seconds: Minimum seconds between points
    :return: Mapping of (invocation_id, runner_id) -> sub_lane index
    """
    result: dict[SubLaneKey, int] = {}

    for runner_id, invocations in invocations_by_runner.items():
        # Sort by first entry timestamp
        sorted_invs = sorted(
            invocations,
            key=lambda inv: inv.sorted_entries[0].timestamp
            if inv.entries
            else datetime.max,
        )

        sub_lanes: list[SubLaneOccupancy] = []

        for inv in sorted_invs:
            if not inv.entries:
                continue

            segments = inv.get_segment_intervals(end_time)
            points = inv.get_point_timestamps()

            # Find first available sub-lane
            assigned_lane = None
            for lane_idx, occupancy in enumerate(sub_lanes):
                if occupancy.can_accommodate(segments, points, point_buffer_seconds):
                    assigned_lane = lane_idx
                    occupancy.add(segments, points)
                    break

            if assigned_lane is None:
                # Need a new sub-lane
                assigned_lane = len(sub_lanes)
                new_occupancy = SubLaneOccupancy()
                new_occupancy.add(segments, points)
                sub_lanes.append(new_occupancy)

            result[SubLaneKey(inv.invocation_id, runner_id)] = assigned_lane

    return result
