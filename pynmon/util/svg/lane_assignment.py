"""
Lane assignment for timeline visualization.

This module assigns vertical lanes to visual elements (segments and points)
to avoid overlap. Each element is a time interval that needs its own space.

Key concepts:
- An element is a time interval [start, end] for a specific status
- If start == end: it's a point (rendered as circle)
- If start != end: it's a segment (rendered as bar)
- Lane assignment is per-element, NOT per-invocation
- Same invocation can have elements in different lanes on the same worker

Critical rules:
1. Connected elements share lanes: If element A ends at time T and element B
   from the same invocation starts at time T, they MUST be on the same lane.
2. Special case: REGISTERED with registered_by_inv_id reuses parent's lane.

Algorithm:
1. Build chains of connected elements (same invocation, end_time == start_time)
2. Assign each chain to a single lane
3. Special handling for REGISTERED linked to parent invocation
"""

from dataclasses import dataclass, field
from datetime import datetime
from typing import NamedTuple


class TimeInterval(NamedTuple):
    """A time interval with start and end."""

    start: datetime
    end: datetime

    @property
    def is_point(self) -> bool:
        """True if this is an instantaneous point (start == end)."""
        return self.start == self.end

    def overlaps(self, other: "TimeInterval") -> bool:
        """
        Check if this interval overlaps with another.

        Two intervals overlap if they share any time period.
        Touching at boundaries (end == start) is NOT overlap.
        """
        # Points don't overlap with anything except exact same timestamp
        if self.is_point and other.is_point:
            return self.start == other.start
        if self.is_point:
            # Point overlaps segment only if STRICTLY inside (not at boundaries)
            return other.start < self.start < other.end
        if other.is_point:
            # Segment overlaps point only if point is STRICTLY inside
            return self.start < other.start < self.end
        # Both are segments - they overlap if one doesn't end before the other starts
        return not (self.end <= other.start or self.start >= other.end)


@dataclass
class VisualElement:
    """
    A visual element on the timeline - either a point or segment.

    This represents a single "thing" that needs a lane assignment.
    """

    invocation_id: str
    runner_id: str
    interval: TimeInterval
    status: str
    # For REGISTERED status, the invocation that registered this one
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


class ElementLaneKey(NamedTuple):
    """
    Key to identify an element for lane lookup.

    Uses (invocation_id, runner_id, start_time) since the same invocation
    can have multiple elements on the same runner at different times.
    """

    invocation_id: str
    runner_id: str
    start_time: datetime


@dataclass
class LaneOccupancy:
    """Tracks intervals occupying a lane."""

    intervals: list[TimeInterval] = field(default_factory=list)

    def can_fit(self, interval: TimeInterval) -> bool:
        """Check if the interval can fit in this lane without overlap."""
        return all(not interval.overlaps(existing) for existing in self.intervals)

    def can_fit_chain(self, intervals: list[TimeInterval]) -> bool:
        """Check if all intervals in a chain can fit in this lane."""
        return all(self.can_fit(interval) for interval in intervals)

    def add(self, interval: TimeInterval) -> None:
        """Add an interval to this lane."""
        self.intervals.append(interval)

    def add_chain(self, intervals: list[TimeInterval]) -> None:
        """Add all intervals in a chain to this lane."""
        self.intervals.extend(intervals)


@dataclass
class ElementChain:
    """
    A chain of connected elements from the same invocation.

    Elements are connected if one ends exactly when the next starts.
    A connected chain must be assigned to the same lane.
    """

    elements: list[VisualElement] = field(default_factory=list)

    @property
    def invocation_id(self) -> str:
        """Invocation ID (all elements in chain have same ID)."""
        return self.elements[0].invocation_id if self.elements else ""

    @property
    def runner_id(self) -> str:
        """Runner ID (all elements in chain are on same runner)."""
        return self.elements[0].runner_id if self.elements else ""

    @property
    def start_time(self) -> datetime:
        """Start time of the chain (first element's start)."""
        return self.elements[0].start_time if self.elements else datetime.min

    @property
    def intervals(self) -> list[TimeInterval]:
        """All intervals in this chain."""
        return [e.interval for e in self.elements]

    def add(self, element: VisualElement) -> None:
        """Add an element to the chain."""
        self.elements.append(element)


def _times_are_connected(end_time: datetime, start_time: datetime) -> bool:
    """
    Check if two times are connected (close enough to be considered sequential).

    Uses a small tolerance to handle microsecond differences from database storage.
    """
    from datetime import timedelta

    # Allow up to 1 millisecond difference for connection
    tolerance = timedelta(milliseconds=1)
    diff = abs((start_time - end_time).total_seconds())
    return diff <= tolerance.total_seconds()


def _build_chains(elements: list[VisualElement]) -> list[ElementChain]:
    """
    Build chains of connected elements.

    Elements are connected if they're from the same invocation and
    one ends when the next starts (with small tolerance for timing precision).
    """
    if not elements:
        return []

    # Sort by invocation_id, then by start_time
    sorted_elements = sorted(elements, key=lambda e: (e.invocation_id, e.start_time))

    chains: list[ElementChain] = []
    current_chain: ElementChain | None = None

    for elem in sorted_elements:
        if current_chain is None:
            # Start a new chain
            current_chain = ElementChain()
            current_chain.add(elem)
        elif (
            elem.invocation_id == current_chain.invocation_id
            and current_chain.elements
            and _times_are_connected(
                current_chain.elements[-1].end_time, elem.start_time
            )
        ):
            # Element is connected to the chain (end time ~= start time)
            current_chain.add(elem)
        else:
            # Not connected - save current chain and start new one
            chains.append(current_chain)
            current_chain = ElementChain()
            current_chain.add(elem)

    # Don't forget the last chain
    if current_chain and current_chain.elements:
        chains.append(current_chain)

    return chains


def assign_lanes(
    elements: list[VisualElement],
) -> dict[ElementLaneKey, int]:
    """
    Assign lanes to visual elements to avoid overlap.

    Connected elements (same invocation, end_time == start_time) are
    assigned to the same lane as a chain.

    :param elements: List of all visual elements
    :return: Mapping of (invocation_id, runner_id, start_time) -> lane index
    """
    # Group elements by runner
    by_runner: dict[str, list[VisualElement]] = {}
    for elem in elements:
        by_runner.setdefault(elem.runner_id, []).append(elem)

    result: dict[ElementLaneKey, int] = {}

    for runner_id, runner_elements in by_runner.items():
        # Build chains of connected elements
        chains = _build_chains(runner_elements)

        # Sort chains by start time
        sorted_chains = sorted(chains, key=lambda c: c.start_time)

        # Track lane occupancy
        lanes: list[LaneOccupancy] = []

        # Track invocation -> (interval, lane) for REGISTERED linking
        inv_lane_at_time: dict[str, list[tuple[TimeInterval, int]]] = {}

        # First pass: assign lanes to chains (skip REGISTERED with parent)
        deferred_chains: list[ElementChain] = []

        for chain in sorted_chains:
            # Check if this is a single REGISTERED element with parent
            if (
                len(chain.elements) == 1
                and chain.elements[0].status.upper() == "REGISTERED"
                and chain.elements[0].registered_by_inv_id
            ):
                deferred_chains.append(chain)
                continue

            assigned = _find_or_create_lane_for_chain(chain.intervals, lanes)

            # Record lane for all elements in the chain
            for elem in chain.elements:
                key = ElementLaneKey(elem.invocation_id, runner_id, elem.start_time)
                result[key] = assigned
                inv_lane_at_time.setdefault(elem.invocation_id, []).append(
                    (elem.interval, assigned)
                )

        # Second pass: handle REGISTERED with registered_by_inv_id
        for chain in deferred_chains:
            elem = chain.elements[0]  # Single element chain
            key = ElementLaneKey(elem.invocation_id, runner_id, elem.start_time)

            # Try to find parent's lane at this time
            parent_inv_id = elem.registered_by_inv_id
            parent_lane = None
            if parent_inv_id:
                parent_lanes = inv_lane_at_time.get(parent_inv_id, [])
                for interval, lane in parent_lanes:
                    # Check if parent has a segment covering this time
                    if interval.start <= elem.start_time <= interval.end:
                        parent_lane = lane
                        break

            if parent_lane is not None:
                # Reuse parent's lane
                lanes[parent_lane].add(elem.interval)
                result[key] = parent_lane
            else:
                # No parent segment at this time, assign normally
                assigned = _find_or_create_lane_for_chain(chain.intervals, lanes)
                result[key] = assigned

            inv_lane_at_time.setdefault(elem.invocation_id, []).append(
                (elem.interval, result[key])
            )

    return result


def _find_or_create_lane_for_chain(
    intervals: list[TimeInterval], lanes: list[LaneOccupancy]
) -> int:
    """Find first available lane for a chain or create a new one."""
    for idx, lane in enumerate(lanes):
        if lane.can_fit_chain(intervals):
            lane.add_chain(intervals)
            return idx

    # Need new lane
    new_lane = LaneOccupancy()
    new_lane.add_chain(intervals)
    lanes.append(new_lane)
    return len(lanes) - 1
