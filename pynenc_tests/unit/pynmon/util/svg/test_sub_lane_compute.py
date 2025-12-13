"""
Tests for compute_sub_lanes function.
"""

from datetime import UTC, datetime

from pynmon.util.svg.sub_lane import InvocationOnRunner, SubLaneKey, compute_sub_lanes


def test_compute_single_invocation_gets_lane_zero() -> None:
    """Single invocation gets sub-lane 0."""
    inv = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv.add_entry(datetime(2025, 1, 1, 12, 0, tzinfo=UTC), "RUNNING")

    invocations_by_runner = {"runner-1": [inv]}
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(invocations_by_runner, end_time)

    assert result[SubLaneKey("inv-1", "runner-1")] == 0


def test_compute_non_overlapping_invocations_share_lane() -> None:
    """Non-overlapping invocations can share sub-lane 0."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 13, 30, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv1.add_entry(t1, "RUNNING")
    inv1.add_entry(t2, "SUCCESS")

    inv2 = InvocationOnRunner(invocation_id="inv-2", runner_id="runner-1")
    inv2.add_entry(t3, "RUNNING")
    inv2.add_entry(t4, "SUCCESS")

    invocations_by_runner = {"runner-1": [inv1, inv2]}
    end_time = datetime(2025, 1, 1, 14, 0, tzinfo=UTC)

    result = compute_sub_lanes(invocations_by_runner, end_time)

    assert result[SubLaneKey("inv-1", "runner-1")] == 0
    assert result[SubLaneKey("inv-2", "runner-1")] == 0


def test_compute_overlapping_invocations_get_different_lanes() -> None:
    """Overlapping invocations get different sub-lanes."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 15, tzinfo=UTC)
    t3 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)
    t4 = datetime(2025, 1, 1, 12, 45, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv1.add_entry(t1, "RUNNING")
    inv1.add_entry(t3, "SUCCESS")

    inv2 = InvocationOnRunner(invocation_id="inv-2", runner_id="runner-1")
    inv2.add_entry(t2, "RUNNING")
    inv2.add_entry(t4, "SUCCESS")

    invocations_by_runner = {"runner-1": [inv1, inv2]}
    end_time = datetime(2025, 1, 1, 14, 0, tzinfo=UTC)

    result = compute_sub_lanes(invocations_by_runner, end_time)

    lane1 = result[SubLaneKey("inv-1", "runner-1")]
    lane2 = result[SubLaneKey("inv-2", "runner-1")]
    assert lane1 != lane2


def test_compute_point_only_invocations_can_share_if_distant() -> None:
    """Point-only invocations can share sub-lane if timestamps are distant."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv1.add_entry(t1, "REGISTERED")

    inv2 = InvocationOnRunner(invocation_id="inv-2", runner_id="runner-1")
    inv2.add_entry(t2, "REGISTERED")

    invocations_by_runner = {"runner-1": [inv1, inv2]}
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(
        invocations_by_runner, end_time, point_buffer_seconds=2.0
    )

    assert result[SubLaneKey("inv-1", "runner-1")] == 0
    assert result[SubLaneKey("inv-2", "runner-1")] == 0


def test_compute_point_only_invocations_separate_if_close() -> None:
    """Point-only invocations get different lanes if timestamps are close."""
    t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv1.add_entry(t1, "REGISTERED")

    inv2 = InvocationOnRunner(invocation_id="inv-2", runner_id="runner-1")
    inv2.add_entry(t2, "REGISTERED")

    invocations_by_runner = {"runner-1": [inv1, inv2]}
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(
        invocations_by_runner, end_time, point_buffer_seconds=2.0
    )

    lane1 = result[SubLaneKey("inv-1", "runner-1")]
    lane2 = result[SubLaneKey("inv-2", "runner-1")]
    assert lane1 != lane2


def test_compute_multiple_runners_independent() -> None:
    """Sub-lanes are computed independently per runner."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv-1", runner_id="runner-1")
    inv1.add_entry(t1, "RUNNING")

    inv2 = InvocationOnRunner(invocation_id="inv-2", runner_id="runner-2")
    inv2.add_entry(t1, "RUNNING")

    invocations_by_runner = {
        "runner-1": [inv1],
        "runner-2": [inv2],
    }
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(invocations_by_runner, end_time)

    assert result[SubLaneKey("inv-1", "runner-1")] == 0
    assert result[SubLaneKey("inv-2", "runner-2")] == 0


def test_compute_same_invocation_different_runners() -> None:
    """Same invocation on different runners gets separate sub-lane entries."""
    t1 = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    t2 = datetime(2025, 1, 1, 12, 30, tzinfo=UTC)

    inv_ext = InvocationOnRunner(invocation_id="inv-1", runner_id="ExternalRunner")
    inv_ext.add_entry(t1, "REGISTERED")

    inv_thread = InvocationOnRunner(invocation_id="inv-1", runner_id="ThreadRunner")
    inv_thread.add_entry(t2, "RUNNING")

    invocations_by_runner = {
        "ExternalRunner": [inv_ext],
        "ThreadRunner": [inv_thread],
    }
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(invocations_by_runner, end_time)

    assert result[SubLaneKey("inv-1", "ExternalRunner")] == 0
    assert result[SubLaneKey("inv-1", "ThreadRunner")] == 0


def test_compute_many_simultaneous_points_stack() -> None:
    """Many simultaneous points get stacked in different sub-lanes."""
    t = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)

    invocations = []
    for i in range(5):
        inv = InvocationOnRunner(invocation_id=f"inv-{i}", runner_id="runner-1")
        inv.add_entry(t, "REGISTERED")
        invocations.append(inv)

    invocations_by_runner = {"runner-1": invocations}
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(
        invocations_by_runner, end_time, point_buffer_seconds=2.0
    )

    lanes = [result[SubLaneKey(f"inv-{i}", "runner-1")] for i in range(5)]
    assert len(set(lanes)) == 5


def test_compute_complex_scenario_reuses_lanes() -> None:
    """Complex scenario: lanes are reused when possible."""
    t0 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t1 = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)
    t10 = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)
    t11 = datetime(2025, 1, 1, 12, 0, 11, tzinfo=UTC)

    inv_a = InvocationOnRunner(invocation_id="inv-a", runner_id="runner-1")
    inv_a.add_entry(t0, "REGISTERED")

    inv_b = InvocationOnRunner(invocation_id="inv-b", runner_id="runner-1")
    inv_b.add_entry(t1, "REGISTERED")

    inv_c = InvocationOnRunner(invocation_id="inv-c", runner_id="runner-1")
    inv_c.add_entry(t10, "REGISTERED")

    inv_d = InvocationOnRunner(invocation_id="inv-d", runner_id="runner-1")
    inv_d.add_entry(t11, "REGISTERED")

    invocations_by_runner = {"runner-1": [inv_a, inv_b, inv_c, inv_d]}
    end_time = datetime(2025, 1, 1, 13, 0, tzinfo=UTC)

    result = compute_sub_lanes(
        invocations_by_runner, end_time, point_buffer_seconds=2.0
    )

    lane_a = result[SubLaneKey("inv-a", "runner-1")]
    lane_b = result[SubLaneKey("inv-b", "runner-1")]
    lane_c = result[SubLaneKey("inv-c", "runner-1")]

    # a and b must be different (close)
    assert lane_a != lane_b
    # c can reuse lane_a (far from a)
    assert lane_c == lane_a
