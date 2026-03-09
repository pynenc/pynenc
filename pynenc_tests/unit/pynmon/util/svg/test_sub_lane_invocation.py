"""
Tests for InvocationOnRunner in sub-lane computation.
"""

from datetime import UTC, datetime

from pynmon.util.svg.sub_lane import InvocationOnRunner, TimeInterval


def test_invocation_get_segment_intervals() -> None:
    """Test segment interval extraction."""
    inv = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    inv.add_entry(t1, "PENDING")
    inv.add_entry(t2, "RUNNING")
    inv.add_entry(t3, "SUCCESS")

    end_time = datetime(2025, 1, 1, 14, 0, tzinfo=UTC)
    intervals = inv.get_segment_intervals(end_time)

    assert len(intervals) == 2
    assert intervals[0] == TimeInterval(start=t1, end=t2)
    assert intervals[1] == TimeInterval(start=t2, end=t3)


def test_invocation_get_point_timestamps() -> None:
    """Test point timestamp extraction."""
    inv = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    inv.add_entry(t1, "REGISTERED")
    inv.add_entry(t2, "RUNNING")
    inv.add_entry(t3, "SUCCESS")

    points = inv.get_point_timestamps()

    assert len(points) == 2
    assert t1 in points
    assert t3 in points


def test_invocation_ongoing_segment_extends_to_end_time() -> None:
    """Test that ongoing segments extend to end_time."""
    inv = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    end_time = datetime(2025, 1, 1, 14, 0, tzinfo=UTC)

    inv.add_entry(t1, "RUNNING")

    intervals = inv.get_segment_intervals(end_time)

    assert len(intervals) == 1
    assert intervals[0] == TimeInterval(start=t1, end=end_time)
