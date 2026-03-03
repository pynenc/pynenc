"""
Core interval types and lane occupancy tracking for timeline assignment.

This module provides the fundamental types needed for lane assignment:
  - TimeInterval: a start/end time window
  - LaneOccupancy: tracks which intervals occupy a lane, with O(log n) overlap check
  - ElementChain / VisualElement: represent items that need lane placement
  - ElementLaneKey: result key mapping element → assigned sub-lane
"""

import bisect
from dataclasses import dataclass, field
from datetime import datetime, timedelta
from typing import NamedTuple


class TimeInterval(NamedTuple):
    """A time interval with start and end."""

    start: datetime
    end: datetime

    @property
    def is_point(self) -> bool:
        """True if this represents an instantaneous moment (start == end)."""
        return self.start == self.end

    def overlaps(self, other: "TimeInterval") -> bool:
        """Check if this interval overlaps with another."""
        if self.is_point and other.is_point:
            return self.start == other.start
        if self.is_point:
            return other.start < self.start < other.end
        if other.is_point:
            return self.start < other.start < self.end
        return not (self.end <= other.start or self.start >= other.end)


class ElementLaneKey(NamedTuple):
    """Lookup key: (invocation_id, runner_id, start_time)."""

    invocation_id: str
    runner_id: str
    start_time: datetime


@dataclass
class VisualElement:
    """A visual element — either a point or a segment — needing a lane."""

    invocation_id: str
    runner_id: str
    interval: TimeInterval
    status: str
    registered_by_inv_id: str | None = None

    @property
    def is_point(self) -> bool:
        """True if this element is a point (instantaneous)."""
        return self.interval.is_point

    @property
    def start_time(self) -> datetime:
        """Start time of this element."""
        return self.interval.start

    @property
    def end_time(self) -> datetime:
        """End time of this element."""
        return self.interval.end


@dataclass
class LaneOccupancy:
    """
    Tracks non-overlapping intervals in a single sub-lane.

    Uses a sorted list + bisect for O(log n) overlap checks instead of
    the naive O(n) linear scan. For 48 k elements this avoids O(n²) lane
    assignment that would freeze the process.
    """

    _starts: list[datetime] = field(default_factory=list)
    _intervals: list[TimeInterval] = field(default_factory=list)

    def can_fit(self, interval: TimeInterval) -> bool:
        """O(log n) overlap check using binary search."""
        pos = bisect.bisect_right(self._starts, interval.start)
        # Check predecessor (its end might extend into our start)
        if pos > 0 and self._intervals[pos - 1].overlaps(interval):
            return False
        # Check successors until they start at or after our end
        for i in range(pos, len(self._intervals)):
            if self._intervals[i].start >= interval.end:
                break
            if self._intervals[i].overlaps(interval):
                return False
        return True

    def can_fit_chain(self, intervals: list[TimeInterval]) -> bool:
        """Check if all intervals in a chain fit without any overlap."""
        return all(self.can_fit(iv) for iv in intervals)

    def add(self, interval: TimeInterval) -> None:
        """Insert interval while keeping the sorted order."""
        pos = bisect.bisect_right(self._starts, interval.start)
        self._starts.insert(pos, interval.start)
        self._intervals.insert(pos, interval)

    def add_chain(self, intervals: list[TimeInterval]) -> None:
        """Add all intervals in a chain."""
        for iv in intervals:
            self.add(iv)


@dataclass
class ElementChain:
    """Chain of connected elements from the same invocation on the same runner."""

    elements: list[VisualElement] = field(default_factory=list)

    @property
    def invocation_id(self) -> str:
        """Invocation ID shared by all elements."""
        return self.elements[0].invocation_id if self.elements else ""

    @property
    def runner_id(self) -> str:
        """Runner ID shared by all elements."""
        return self.elements[0].runner_id if self.elements else ""

    @property
    def start_time(self) -> datetime:
        """Start time of the first element."""
        return self.elements[0].start_time if self.elements else datetime.min

    @property
    def intervals(self) -> list[TimeInterval]:
        """All time intervals in this chain."""
        return [e.interval for e in self.elements]

    def add(self, element: VisualElement) -> None:
        """Add an element to the chain."""
        self.elements.append(element)


_CONNECTION_TOLERANCE = timedelta(milliseconds=1)


def times_are_connected(t1: datetime, t2: datetime) -> bool:
    """True if two times are within 1 ms — considered the same transition point."""
    return abs((t2 - t1).total_seconds()) <= _CONNECTION_TOLERANCE.total_seconds()


def build_chains(elements: list[VisualElement]) -> list[ElementChain]:
    """
    Group connected elements into chains.

    Elements are connected when the end of one equals the start of the next
    (within tolerance) and both belong to the same invocation.
    """
    if not elements:
        return []
    sorted_elems = sorted(elements, key=lambda e: (e.invocation_id, e.start_time))
    chains: list[ElementChain] = []
    current: ElementChain | None = None
    for elem in sorted_elems:
        if current is None:
            current = ElementChain()
            current.add(elem)
        elif (
            elem.invocation_id == current.invocation_id
            and current.elements
            and times_are_connected(current.elements[-1].end_time, elem.start_time)
        ):
            current.add(elem)
        else:
            chains.append(current)
            current = ElementChain()
            current.add(elem)
    if current and current.elements:
        chains.append(current)
    return chains
