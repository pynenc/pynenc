"""
Tests for SubLaneOccupancy in sub-lane computation.
"""

from datetime import UTC, datetime

from pynmon.util.svg.sub_lane import SubLaneOccupancy, TimeInterval


def test_occupancy_empty_can_accommodate_anything() -> None:
    """Empty sub-lane can accommodate anything."""
    occupancy = SubLaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    assert occupancy.can_accommodate(
        segments=[TimeInterval(t1, t2)],
        points=[t1],
    )


def test_occupancy_segment_blocks_overlapping_segment() -> None:
    """Existing segment blocks overlapping new segment."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 13, 30, tzinfo=UTC)

    occupancy = SubLaneOccupancy()
    occupancy.add(segments=[TimeInterval(t1, t2)], points=[])

    assert not occupancy.can_accommodate(
        segments=[TimeInterval(t3, t4)],
        points=[],
    )


def test_occupancy_segment_blocks_point_inside() -> None:
    """Existing segment blocks point inside it."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t_middle = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)

    occupancy = SubLaneOccupancy()
    occupancy.add(segments=[TimeInterval(t1, t2)], points=[])

    assert not occupancy.can_accommodate(
        segments=[],
        points=[t_middle],
    )


def test_occupancy_point_blocks_nearby_point() -> None:
    """Existing point blocks nearby point (within buffer)."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)

    occupancy = SubLaneOccupancy()
    occupancy.add(segments=[], points=[t1])

    assert not occupancy.can_accommodate(
        segments=[],
        points=[t2],
        point_buffer_seconds=2.0,
    )


def test_occupancy_distant_points_can_share() -> None:
    """Distant points can share the same sub-lane."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)

    occupancy = SubLaneOccupancy()
    occupancy.add(segments=[], points=[t1])

    assert occupancy.can_accommodate(
        segments=[],
        points=[t2],
        point_buffer_seconds=2.0,
    )


def test_occupancy_non_overlapping_segments_can_share() -> None:
    """Non-overlapping segments can share the same sub-lane."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 13, 30, tzinfo=UTC)

    occupancy = SubLaneOccupancy()
    occupancy.add(segments=[TimeInterval(t1, t2)], points=[])

    assert occupancy.can_accommodate(
        segments=[TimeInterval(t3, t4)],
        points=[],
    )
