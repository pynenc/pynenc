"""
Tests for sub-lane computation using real captured data.
"""

from datetime import datetime

from pynmon.util.svg.sub_lane import (
    InvocationOnRunner,
    RunnerEntry,
    SubLaneKey,
    compute_sub_lanes,
)


def test_external_runner_points_reuse_sublanes(
    real_data: dict[str, list[InvocationOnRunner]],
    real_data_end_time: datetime,
    real_data_external_runner: str,
) -> None:
    """
    ExternalRunner has 12 REGISTERED points spread over ~1.1 seconds.

    With point_buffer_seconds=2.0, all points within 2 seconds conflict.
    With smaller buffer, more can share sub-lanes.
    """
    external_runner = real_data_external_runner

    result_2s = compute_sub_lanes(
        real_data, real_data_end_time, point_buffer_seconds=2.0
    )

    external_sub_lanes_2s = [
        result_2s[SubLaneKey(inv.invocation_id, external_runner)]
        for inv in real_data[external_runner]
    ]
    max_sub_lane_2s = max(external_sub_lanes_2s)

    result_small = compute_sub_lanes(
        real_data, real_data_end_time, point_buffer_seconds=0.01
    )

    external_sub_lanes_small = [
        result_small[SubLaneKey(inv.invocation_id, external_runner)]
        for inv in real_data[external_runner]
    ]
    max_sub_lane_small = max(external_sub_lanes_small)

    assert max_sub_lane_small < max_sub_lane_2s, (
        f"Smaller buffer should allow more sub-lane reuse. "
        f"Got max={max_sub_lane_small} with 10ms vs max={max_sub_lane_2s} with 2s"
    )


def test_thread_runner_segments_reuse_after_completion(
    real_data: dict[str, list[InvocationOnRunner]],
    real_data_end_time: datetime,
    real_data_thread_runner: str,
) -> None:
    """
    ThreadRunner has segments that complete before others start.

    inv 61a7740a starts after 4ab5089c and 48977619 complete,
    so it should be able to reuse one of their sub-lanes.
    """
    thread_runner = real_data_thread_runner

    result = compute_sub_lanes(real_data, real_data_end_time, point_buffer_seconds=0.01)

    thread_sub_lanes = {
        inv.invocation_id: result[SubLaneKey(inv.invocation_id, thread_runner)]
        for inv in real_data[thread_runner]
    }

    short_task_1 = "4ab5089c-b3a1-48a1-ab33-cb8e3645103b"
    short_task_2 = "48977619-5423-425e-8dcd-3bc8fd2dbabb"
    late_task = "61a7740a-6d19-4b9c-b664-5f5844180cb2"

    short_lanes = {thread_sub_lanes[short_task_1], thread_sub_lanes[short_task_2]}
    late_lane = thread_sub_lanes[late_task]

    assert late_lane in short_lanes, (
        f"Late task should reuse sub-lane from tasks that ended earlier. "
        f"Got late_lane={late_lane}, short_lanes={short_lanes}"
    )


def test_point_buffer_should_be_visual_not_temporal(
    real_data: dict[str, list[InvocationOnRunner]],
    real_data_end_time: datetime,
    real_data_external_runner: str,
) -> None:
    """Smaller buffers should generally result in fewer sub-lanes."""
    external_runner = real_data_external_runner

    buffers = [0.001, 0.01, 0.05, 0.1, 0.5, 1.0, 2.0]
    results = {}

    for buffer in buffers:
        result = compute_sub_lanes(
            real_data, real_data_end_time, point_buffer_seconds=buffer
        )
        max_lane = max(
            result[SubLaneKey(inv.invocation_id, external_runner)]
            for inv in real_data[external_runner]
        )
        results[buffer] = max_lane

    assert results[0.01] <= results[2.0], (
        f"Smaller buffer should not increase sub-lanes. "
        f"Got {results[0.01]} with 10ms vs {results[2.0]} with 2s"
    )


def test_millisecond_apart_points_with_tiny_buffer() -> None:
    """Points 7ms apart should share sub-lane with 1ms buffer."""
    from datetime import UTC

    runner = "runner1"

    inv1 = InvocationOnRunner(invocation_id="inv1", runner_id=runner, entries=[])
    inv1.entries.append(
        RunnerEntry(
            timestamp=datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC), status="registered"
        )
    )

    inv2 = InvocationOnRunner(invocation_id="inv2", runner_id=runner, entries=[])
    inv2.entries.append(
        RunnerEntry(
            timestamp=datetime(2025, 1, 1, 0, 0, 0, 7000, tzinfo=UTC),
            status="registered",
        )
    )

    end_time = datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)

    result = compute_sub_lanes(
        {runner: [inv1, inv2]}, end_time, point_buffer_seconds=0.001
    )

    lane1 = result[SubLaneKey("inv1", runner)]
    lane2 = result[SubLaneKey("inv2", runner)]

    assert lane1 == lane2, (
        f"Points 7ms apart with 1ms buffer should share lane. Got {lane1}, {lane2}"
    )


def test_same_timestamp_points_always_conflict() -> None:
    """Points at exact same time always need separate sub-lanes."""
    from datetime import UTC

    runner = "runner1"
    same_time = datetime(2025, 1, 1, 0, 0, 0, 0, tzinfo=UTC)

    inv1 = InvocationOnRunner(invocation_id="inv1", runner_id=runner, entries=[])
    inv1.entries.append(RunnerEntry(timestamp=same_time, status="registered"))

    inv2 = InvocationOnRunner(invocation_id="inv2", runner_id=runner, entries=[])
    inv2.entries.append(RunnerEntry(timestamp=same_time, status="registered"))

    end_time = datetime(2025, 1, 1, 0, 0, 1, tzinfo=UTC)

    result = compute_sub_lanes(
        {runner: [inv1, inv2]}, end_time, point_buffer_seconds=0.0001
    )

    lane1 = result[SubLaneKey("inv1", runner)]
    lane2 = result[SubLaneKey("inv2", runner)]

    # Same timestamp points should conflict
    assert lane1 != lane2, (
        f"Same timestamp points should conflict. Got {lane1}, {lane2}"
    )
