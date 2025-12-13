"""
Tests for lane assignment module with real-world scenarios.

These tests verify that visual elements are correctly assigned to lanes
to avoid overlap while maximizing space reuse.
"""

from datetime import UTC, datetime, timedelta


from pynmon.util.svg.lane_assignment import (
    ElementLaneKey,
    LaneOccupancy,
    TimeInterval,
    VisualElement,
    assign_lanes,
)


# TimeInterval tests
def test_point_interval_is_point() -> None:
    """A point interval has start == end."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    interval = TimeInterval(t, t)
    assert interval.is_point is True


def test_segment_interval_is_not_point() -> None:
    """A segment interval has start != end."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    interval = TimeInterval(t1, t2)
    assert interval.is_point is False


def test_points_at_same_time_overlap() -> None:
    """Two points at the exact same time overlap."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    p1 = TimeInterval(t, t)
    p2 = TimeInterval(t, t)
    assert p1.overlaps(p2) is True


def test_points_at_different_times_do_not_overlap() -> None:
    """Two points at different times do not overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)
    p1 = TimeInterval(t1, t1)
    p2 = TimeInterval(t2, t2)
    assert p1.overlaps(p2) is False


def test_segments_overlap() -> None:
    """Overlapping segments are detected."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)
    s1 = TimeInterval(t1, t2)  # 12:00 - 12:02
    s2 = TimeInterval(t3, t4)  # 12:01 - 12:03
    assert s1.overlaps(s2) is True


def test_segments_do_not_overlap() -> None:
    """Non-overlapping segments are detected."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)
    s1 = TimeInterval(t1, t2)  # 12:00 - 12:01
    s2 = TimeInterval(t3, t4)  # 12:02 - 12:03
    assert s1.overlaps(s2) is False


def test_point_inside_segment_overlaps() -> None:
    """A point inside a segment overlaps with it."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t_mid = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    segment = TimeInterval(t1, t2)
    point = TimeInterval(t_mid, t_mid)
    assert segment.overlaps(point) is True
    assert point.overlaps(segment) is True


def test_point_outside_segment_does_not_overlap() -> None:
    """A point outside a segment does not overlap."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t_after = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)
    segment = TimeInterval(t1, t2)
    point = TimeInterval(t_after, t_after)
    assert segment.overlaps(point) is False
    assert point.overlaps(segment) is False


# LaneOccupancy tests
def test_lane_can_fit_non_overlapping() -> None:
    """Lane can fit intervals that don't overlap."""
    lane = LaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    lane.add(TimeInterval(t1, t2))
    assert lane.can_fit(TimeInterval(t3, t4)) is True


def test_lane_cannot_fit_overlapping() -> None:
    """Lane cannot fit overlapping intervals."""
    lane = LaneOccupancy()
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    lane.add(TimeInterval(t1, t2))
    assert lane.can_fit(TimeInterval(t3, t4)) is False


# Real-world scenario tests
def test_points_at_different_times_reuse_lane(
    base_time: datetime, external_runner: str
) -> None:
    """
    Points at different times should reuse the same lane.

    This is the core bug: REGISTERED events on ExternalRunner at different
    times were each getting their own lane instead of sharing.
    """
    elements = [
        VisualElement(
            invocation_id=f"inv-{i}",
            runner_id=external_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=i * 10),
                base_time + timedelta(seconds=i * 10),
            ),
            status="REGISTERED",
        )
        for i in range(5)
    ]

    result = assign_lanes(elements)

    # All points should be in lane 0 since they don't overlap
    lanes = set(result.values())
    assert lanes == {0}, f"Expected all points in lane 0, got lanes: {result}"


def test_overlapping_segments_get_different_lanes(
    base_time: datetime, thread_runner: str
) -> None:
    """Overlapping segments must be in different lanes."""
    elements = [
        VisualElement(
            invocation_id="inv-1",
            runner_id=thread_runner,
            interval=TimeInterval(base_time, base_time + timedelta(seconds=3)),
            status="RUNNING",
        ),
        VisualElement(
            invocation_id="inv-2",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=1), base_time + timedelta(seconds=4)
            ),
            status="RUNNING",
        ),
    ]

    result = assign_lanes(elements)

    lanes = list(result.values())
    assert lanes[0] != lanes[1], "Overlapping segments should be in different lanes"


def test_sequential_segments_reuse_lane(
    base_time: datetime, thread_runner: str
) -> None:
    """
    Sequential segments (one finishes before next starts) should reuse lanes.

    This is another key bug: segments that don't overlap in time should
    be able to share a lane.
    """
    elements = [
        # First segment: 0-2 seconds
        VisualElement(
            invocation_id="inv-1",
            runner_id=thread_runner,
            interval=TimeInterval(base_time, base_time + timedelta(seconds=2)),
            status="RUNNING",
        ),
        # Second segment: 3-5 seconds (starts after first ends)
        VisualElement(
            invocation_id="inv-2",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=3), base_time + timedelta(seconds=5)
            ),
            status="RUNNING",
        ),
    ]

    result = assign_lanes(elements)

    # Both should be in lane 0
    lanes = list(result.values())
    assert lanes == [0, 0], f"Sequential segments should share lane, got {lanes}"


def test_real_external_runner_scenario(base_time: datetime) -> None:
    """
    Test with real data: 12 REGISTERED events on ExternalRunner at different times.

    Based on actual data showing points spread over ~1.1 seconds.
    """
    external_runner = "ExternalRunner@Krossovers-MacBook-Pro.local-1504"

    # Times from actual data (millisecond precision)
    times_ms = [
        44552,
        51913,
        572137,
        579959,
        586364,
        1109158,
        1120402,
        1128063,
        1135217,
        1143924,
        1149631,
        1156642,
    ]

    elements = [
        VisualElement(
            invocation_id=f"inv-{i}",
            runner_id=external_runner,
            interval=TimeInterval(
                base_time + timedelta(microseconds=t),
                base_time + timedelta(microseconds=t),  # Point: start == end
            ),
            status="REGISTERED",
        )
        for i, t in enumerate(times_ms)
    ]

    result = assign_lanes(elements)

    # Points don't overlap in time, so they should all fit in lane 0
    lanes = set(result.values())
    assert lanes == {0}, (
        f"Non-overlapping points should all be in lane 0, got {len(lanes)} lanes"
    )


def test_real_thread_runner_scenario(base_time: datetime) -> None:
    """
    Test with real data: overlapping RUNNING segments on ThreadRunner.

    Some segments overlap (parallel execution), others are sequential.
    """
    thread_runner = "ThreadRunner@Krossovers-MacBook-Pro.local-1504"

    # Simplified: 3 overlapping segments, 1 sequential
    elements = [
        # Segment 1: 0-3 seconds
        VisualElement(
            invocation_id="inv-1",
            runner_id=thread_runner,
            interval=TimeInterval(base_time, base_time + timedelta(seconds=3)),
            status="RUNNING",
        ),
        # Segment 2: 0.1-3 seconds (overlaps with 1)
        VisualElement(
            invocation_id="inv-2",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(milliseconds=100),
                base_time + timedelta(seconds=3),
            ),
            status="RUNNING",
        ),
        # Segment 3: 0.5-2 seconds (overlaps with 1 and 2)
        VisualElement(
            invocation_id="inv-3",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(milliseconds=500),
                base_time + timedelta(seconds=2),
            ),
            status="RUNNING",
        ),
        # Segment 4: 4-5 seconds (sequential, after all others)
        VisualElement(
            invocation_id="inv-4",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=4),
                base_time + timedelta(seconds=5),
            ),
            status="RUNNING",
        ),
    ]

    result = assign_lanes(elements)

    # First 3 overlap, need different lanes
    lane_1 = result[ElementLaneKey("inv-1", thread_runner, base_time)]
    lane_2 = result[
        ElementLaneKey("inv-2", thread_runner, base_time + timedelta(milliseconds=100))
    ]
    lane_3 = result[
        ElementLaneKey("inv-3", thread_runner, base_time + timedelta(milliseconds=500))
    ]
    lane_4 = result[
        ElementLaneKey("inv-4", thread_runner, base_time + timedelta(seconds=4))
    ]

    # Overlapping segments need different lanes
    assert len({lane_1, lane_2, lane_3}) == 3, (
        "Overlapping segments need different lanes"
    )

    # Sequential segment can reuse a lane
    assert lane_4 in {lane_1, lane_2, lane_3}, (
        f"Sequential segment should reuse existing lane, got new lane {lane_4}"
    )


def test_mixed_points_and_segments(base_time: datetime, thread_runner: str) -> None:
    """
    Test mixed scenario with both points and segments.

    Points should be able to coexist in lanes where segments don't cover them.
    """
    elements = [
        # Segment: 0-2 seconds
        VisualElement(
            invocation_id="inv-1",
            runner_id=thread_runner,
            interval=TimeInterval(base_time, base_time + timedelta(seconds=2)),
            status="RUNNING",
        ),
        # Point at 1 second (inside segment - needs different lane)
        VisualElement(
            invocation_id="inv-2",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=1),
                base_time + timedelta(seconds=1),
            ),
            status="REGISTERED",
        ),
        # Point at 3 seconds (after segment - can share lane)
        VisualElement(
            invocation_id="inv-3",
            runner_id=thread_runner,
            interval=TimeInterval(
                base_time + timedelta(seconds=3),
                base_time + timedelta(seconds=3),
            ),
            status="REGISTERED",
        ),
    ]

    result = assign_lanes(elements)

    lane_1 = result[ElementLaneKey("inv-1", thread_runner, base_time)]
    lane_2 = result[
        ElementLaneKey("inv-2", thread_runner, base_time + timedelta(seconds=1))
    ]
    lane_3 = result[
        ElementLaneKey("inv-3", thread_runner, base_time + timedelta(seconds=3))
    ]

    # Point at 1s is inside segment, needs different lane
    assert lane_1 != lane_2, "Point inside segment needs different lane"

    # Point at 3s is after segment, can share lane
    assert lane_3 == lane_1, "Point after segment should share lane"
