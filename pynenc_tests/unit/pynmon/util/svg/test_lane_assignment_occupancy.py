"""
Tests for LaneOccupancy in lane assignment.
"""

from datetime import UTC, datetime

from pynmon.util.svg.lane_assignment import LaneOccupancy, TimeInterval


def test_lane_occupancy_empty_lane_fits_anything() -> None:
    """Empty lane can fit any interval."""
    lane = LaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    assert lane.can_fit(TimeInterval(t1, t2))


def test_lane_occupancy_rejects_overlap() -> None:
    """Lane rejects overlapping intervals."""
    lane = LaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    lane.add(TimeInterval(t1, t2))
    assert not lane.can_fit(TimeInterval(t3, t4))


def test_lane_occupancy_accepts_sequential() -> None:
    """Lane accepts non-overlapping intervals."""
    lane = LaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    lane.add(TimeInterval(t1, t2))
    assert lane.can_fit(TimeInterval(t3, t4))
