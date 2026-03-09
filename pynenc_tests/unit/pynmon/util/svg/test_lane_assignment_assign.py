"""
Tests for assign_lanes function.
"""

from datetime import UTC, datetime

from pynmon.util.svg.lane_assignment import (
    ElementLaneKey,
    TimeInterval,
    VisualElement,
    assign_lanes,
)


def test_assign_single_element_gets_lane_0() -> None:
    """A single element is assigned to lane 0."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    elem = VisualElement(
        invocation_id="inv-1",
        runner_id="runner-1",
        interval=TimeInterval(t, t),
        status="REGISTERED",
    )
    result = assign_lanes([elem])
    key = ElementLaneKey("inv-1", "runner-1", t)
    assert result[key] == 0


def test_assign_non_overlapping_points_share_lane() -> None:
    """Points at different times share lane 0."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t1, t1), "REGISTERED"),
        VisualElement("inv-2", "runner-1", TimeInterval(t2, t2), "REGISTERED"),
        VisualElement("inv-3", "runner-1", TimeInterval(t3, t3), "REGISTERED"),
    ]
    result = assign_lanes(elements)

    assert result[ElementLaneKey("inv-1", "runner-1", t1)] == 0
    assert result[ElementLaneKey("inv-2", "runner-1", t2)] == 0
    assert result[ElementLaneKey("inv-3", "runner-1", t3)] == 0


def test_assign_overlapping_segments_different_lanes() -> None:
    """Overlapping segments get different lanes."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 3, 0, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t1, t2), "RUNNING"),
        VisualElement("inv-2", "runner-1", TimeInterval(t3, t4), "RUNNING"),
    ]
    result = assign_lanes(elements)

    lane1 = result[ElementLaneKey("inv-1", "runner-1", t1)]
    lane2 = result[ElementLaneKey("inv-2", "runner-1", t3)]
    assert lane1 != lane2


def test_assign_sequential_segments_share_lane() -> None:
    """Sequential (non-overlapping) segments share the same lane."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t1, t2), "RUNNING"),
        VisualElement("inv-2", "runner-1", TimeInterval(t3, t4), "RUNNING"),
    ]
    result = assign_lanes(elements)

    lane1 = result[ElementLaneKey("inv-1", "runner-1", t1)]
    lane2 = result[ElementLaneKey("inv-2", "runner-1", t3)]
    assert lane1 == lane2 == 0


def test_assign_connected_segment_and_point_share_lane() -> None:
    """A segment and its terminating point must share the same lane."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, 581000, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 0, 1, 584000, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t1, t2), "RUNNING"),
        VisualElement("inv-1", "runner-1", TimeInterval(t2, t2), "SUCCESS"),
        VisualElement("inv-2", "runner-1", TimeInterval(t1, t3), "RUNNING"),
        VisualElement("inv-2", "runner-1", TimeInterval(t3, t3), "SUCCESS"),
    ]

    result = assign_lanes(elements)

    lane_inv1_running = result[ElementLaneKey("inv-1", "runner-1", t1)]
    lane_inv1_success = result[ElementLaneKey("inv-1", "runner-1", t2)]
    assert lane_inv1_running == lane_inv1_success

    lane_inv2_running = result[ElementLaneKey("inv-2", "runner-1", t1)]
    lane_inv2_success = result[ElementLaneKey("inv-2", "runner-1", t3)]
    assert lane_inv2_running == lane_inv2_success

    assert lane_inv1_running != lane_inv2_running


def test_assign_full_invocation_chain_shares_lane() -> None:
    """A complete invocation chain shares one lane."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 0, 2, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t1, t2), "PENDING"),
        VisualElement("inv-1", "runner-1", TimeInterval(t2, t3), "RUNNING"),
        VisualElement("inv-1", "runner-1", TimeInterval(t3, t3), "SUCCESS"),
    ]

    result = assign_lanes(elements)

    lane_pending = result[ElementLaneKey("inv-1", "runner-1", t1)]
    lane_running = result[ElementLaneKey("inv-1", "runner-1", t2)]
    lane_success = result[ElementLaneKey("inv-1", "runner-1", t3)]

    assert lane_pending == lane_running == lane_success == 0


def test_assign_real_data_external_runner_points_share_lanes() -> None:
    """REGISTERED points at different times should share lanes."""
    base = datetime(2025, 11, 26, 21, 10, 43, tzinfo=UTC)
    timestamps = [
        base.replace(microsecond=44552),
        base.replace(microsecond=51913),
        base.replace(microsecond=572137),
        base.replace(microsecond=579959),
        base.replace(microsecond=586364),
        base.replace(second=44, microsecond=109158),
        base.replace(second=44, microsecond=120402),
        base.replace(second=44, microsecond=128063),
        base.replace(second=44, microsecond=135217),
        base.replace(second=44, microsecond=143924),
        base.replace(second=44, microsecond=149631),
        base.replace(second=44, microsecond=156642),
    ]

    elements = [
        VisualElement(
            invocation_id=f"inv-{i}",
            runner_id="ExternalRunner@host",
            interval=TimeInterval(ts, ts),
            status="REGISTERED",
        )
        for i, ts in enumerate(timestamps)
    ]

    result = assign_lanes(elements)

    lanes = [
        result[ElementLaneKey(f"inv-{i}", "ExternalRunner@host", ts)]
        for i, ts in enumerate(timestamps)
    ]
    assert all(lane == 0 for lane in lanes), f"Expected all lane 0, got {lanes}"


def test_assign_registered_with_parent_reuses_parent_lane() -> None:
    """REGISTERED with registered_by_inv_id reuses parent's lane."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 1, 0, tzinfo=UTC)

    elements = [
        VisualElement("parent", "runner-1", TimeInterval(t1, t3), "RUNNING"),
        VisualElement(
            "child",
            "runner-1",
            TimeInterval(t2, t2),
            "REGISTERED",
            registered_by_inv_id="parent",
        ),
    ]

    result = assign_lanes(elements)

    parent_lane = result[ElementLaneKey("parent", "runner-1", t1)]
    child_lane = result[ElementLaneKey("child", "runner-1", t2)]

    assert child_lane == parent_lane


def test_assign_different_runners_independent_lanes() -> None:
    """Elements on different runners have independent lane assignments."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    elements = [
        VisualElement("inv-1", "runner-1", TimeInterval(t, t), "REGISTERED"),
        VisualElement("inv-2", "runner-2", TimeInterval(t, t), "REGISTERED"),
    ]

    result = assign_lanes(elements)

    assert result[ElementLaneKey("inv-1", "runner-1", t)] == 0
    assert result[ElementLaneKey("inv-2", "runner-2", t)] == 0
