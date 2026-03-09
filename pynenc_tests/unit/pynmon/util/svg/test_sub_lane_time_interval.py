"""
Tests for TimeInterval in sub-lane computation.
"""

from datetime import UTC, datetime

from pynmon.util.svg.sub_lane import TimeInterval


def test_interval_no_overlap_before() -> None:
    """Intervals that don't overlap (first ends before second starts)."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 13, 30, tzinfo=UTC)

    interval1 = TimeInterval(start=t1, end=t2)
    interval2 = TimeInterval(start=t3, end=t4)

    assert not interval1.overlaps(interval2)
    assert not interval2.overlaps(interval1)


def test_interval_no_overlap_adjacent() -> None:
    """Adjacent intervals (end equals start) don't overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    interval1 = TimeInterval(start=t1, end=t2)
    interval2 = TimeInterval(start=t2, end=t3)

    assert not interval1.overlaps(interval2)
    assert not interval2.overlaps(interval1)


def test_interval_overlap_partial() -> None:
    """Partially overlapping intervals."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 15, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 45, tzinfo=UTC)

    interval1 = TimeInterval(start=t1, end=t2)
    interval2 = TimeInterval(start=t3, end=t4)

    assert interval1.overlaps(interval2)
    assert interval2.overlaps(interval1)


def test_interval_overlap_contained() -> None:
    """One interval contains the other."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 15, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 45, tzinfo=UTC)

    outer = TimeInterval(start=t1, end=t2)
    inner = TimeInterval(start=t3, end=t4)

    assert outer.overlaps(inner)
    assert inner.overlaps(outer)
