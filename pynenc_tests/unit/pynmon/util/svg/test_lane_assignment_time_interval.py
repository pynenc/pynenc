"""
Tests for TimeInterval in lane assignment.
"""

from datetime import UTC, datetime

from pynmon.util.svg.lane_assignment import TimeInterval


def test_time_interval_point_is_point() -> None:
    """A point has start == end."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    interval = TimeInterval(t, t)
    assert interval.is_point


def test_time_interval_segment_is_not_point() -> None:
    """A segment has start != end."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    interval = TimeInterval(t1, t2)
    assert not interval.is_point


def test_time_interval_points_same_time_overlap() -> None:
    """Two points at the same time overlap."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    p1 = TimeInterval(t, t)
    p2 = TimeInterval(t, t)
    assert p1.overlaps(p2)


def test_time_interval_points_different_times_no_overlap() -> None:
    """Two points at different times don't overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)
    p1 = TimeInterval(t1, t1)
    p2 = TimeInterval(t2, t2)
    assert not p1.overlaps(p2)


def test_time_interval_segments_overlap() -> None:
    """Overlapping segments are detected."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    s1 = TimeInterval(t1, t2)
    s2 = TimeInterval(t3, t4)
    assert s1.overlaps(s2)


def test_time_interval_segments_no_overlap_sequential() -> None:
    """Sequential segments don't overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)

    s1 = TimeInterval(t1, t2)
    s2 = TimeInterval(t3, t4)
    assert not s1.overlaps(s2)


def test_time_interval_point_inside_segment_overlaps() -> None:
    """A point inside a segment overlaps with it."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t_mid = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)

    segment = TimeInterval(t1, t2)
    point = TimeInterval(t_mid, t_mid)
    assert point.overlaps(segment)
    assert segment.overlaps(point)


def test_time_interval_point_outside_segment_no_overlap() -> None:
    """A point outside a segment doesn't overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t_after = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)

    segment = TimeInterval(t1, t2)
    point = TimeInterval(t_after, t_after)
    assert not point.overlaps(segment)
