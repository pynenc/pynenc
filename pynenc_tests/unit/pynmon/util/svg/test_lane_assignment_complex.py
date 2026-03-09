"""
Tests for complex real-world lane assignment scenarios.
"""

from datetime import UTC, datetime

from pynmon.util.svg.lane_assignment import (
    ElementLaneKey,
    TimeInterval,
    VisualElement,
    assign_lanes,
)


def test_complex_pause_resume_scenario() -> None:
    """
    Test the complex scenario:

    - inv-0: PENDING-REROUTED on worker-0 (small segment)
    - inv-0: PENDING-RUNNING-PAUSED on worker-1
    - inv-1: PENDING-RUNNING-RETRY on worker-1 (while inv-0 paused, same lane)
    - inv-0: RESUMED-KILLED on worker-1 (same lane)
    """
    w0_t1 = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    w0_t2 = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)

    w1_t1 = datetime(2025, 1, 1, 12, 0, 20, tzinfo=UTC)
    w1_t2 = datetime(2025, 1, 1, 12, 0, 21, tzinfo=UTC)
    w1_t3 = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
    w1_t4 = datetime(2025, 1, 1, 12, 0, 31, tzinfo=UTC)
    w1_t5 = datetime(2025, 1, 1, 12, 0, 32, tzinfo=UTC)
    w1_t6 = datetime(2025, 1, 1, 12, 0, 40, tzinfo=UTC)
    w1_t7 = datetime(2025, 1, 1, 12, 0, 50, tzinfo=UTC)

    elements = [
        VisualElement("inv-0", "worker-0", TimeInterval(w0_t1, w0_t2), "PENDING"),
        VisualElement("inv-0", "worker-0", TimeInterval(w0_t2, w0_t2), "REROUTED"),
        VisualElement("inv-0", "worker-1", TimeInterval(w1_t1, w1_t2), "PENDING"),
        VisualElement("inv-0", "worker-1", TimeInterval(w1_t2, w1_t3), "RUNNING"),
        VisualElement("inv-0", "worker-1", TimeInterval(w1_t3, w1_t3), "PAUSED"),
        VisualElement("inv-1", "worker-1", TimeInterval(w1_t4, w1_t5), "PENDING"),
        VisualElement("inv-1", "worker-1", TimeInterval(w1_t5, w1_t6), "RUNNING"),
        VisualElement("inv-1", "worker-1", TimeInterval(w1_t6, w1_t6), "RETRY"),
        VisualElement("inv-0", "worker-1", TimeInterval(w1_t6, w1_t7), "RESUMED"),
        VisualElement("inv-0", "worker-1", TimeInterval(w1_t7, w1_t7), "KILLED"),
    ]

    result = assign_lanes(elements)

    assert result[ElementLaneKey("inv-0", "worker-0", w0_t1)] == 0

    lane_inv0_pending = result[ElementLaneKey("inv-0", "worker-1", w1_t1)]
    lane_inv0_running = result[ElementLaneKey("inv-0", "worker-1", w1_t2)]
    lane_inv1_pending = result[ElementLaneKey("inv-1", "worker-1", w1_t4)]
    lane_inv0_resumed = result[ElementLaneKey("inv-0", "worker-1", w1_t6)]

    assert lane_inv0_pending is not None
    assert lane_inv0_running is not None
    assert lane_inv1_pending is not None
    assert lane_inv0_resumed is not None


def test_thread_runner_segments_reuse_after_completion() -> None:
    """Segments that finish before others start should reuse lanes."""
    t1_start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t1_end = datetime(2025, 1, 1, 12, 0, 1, tzinfo=UTC)

    t2_start = datetime(2025, 1, 1, 12, 0, 0, microsecond=100000, tzinfo=UTC)
    t2_end = datetime(2025, 1, 1, 12, 0, 1, microsecond=100000, tzinfo=UTC)

    t3_start = datetime(2025, 1, 1, 12, 0, 2, tzinfo=UTC)
    t3_end = datetime(2025, 1, 1, 12, 0, 10, tzinfo=UTC)

    elements = [
        VisualElement(
            "inv-1", "ThreadRunner", TimeInterval(t1_start, t1_end), "RUNNING"
        ),
        VisualElement(
            "inv-2", "ThreadRunner", TimeInterval(t2_start, t2_end), "RUNNING"
        ),
        VisualElement(
            "inv-3", "ThreadRunner", TimeInterval(t3_start, t3_end), "RUNNING"
        ),
    ]

    result = assign_lanes(elements)

    lane1 = result[ElementLaneKey("inv-1", "ThreadRunner", t1_start)]
    lane2 = result[ElementLaneKey("inv-2", "ThreadRunner", t2_start)]
    lane3 = result[ElementLaneKey("inv-3", "ThreadRunner", t3_start)]

    assert lane1 != lane2
    assert lane3 in (lane1, lane2), (
        f"inv-3 should reuse lane {lane1} or {lane2}, got {lane3}"
    )


def test_same_invocation_multiple_elements_different_lanes() -> None:
    """Same invocation can have elements in different lanes on same runner."""
    t1_run_start = datetime(2025, 1, 1, 12, 0, 0, tzinfo=UTC)
    t1_run_end = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
    t1_paused = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
    t1_resume_start = datetime(2025, 1, 1, 12, 1, 30, tzinfo=UTC)
    t1_resume_end = datetime(2025, 1, 1, 12, 2, 0, tzinfo=UTC)

    t2_start = datetime(2025, 1, 1, 12, 0, 30, tzinfo=UTC)
    t2_end = datetime(2025, 1, 1, 12, 1, 30, tzinfo=UTC)

    elements = [
        VisualElement(
            "inv-1", "runner-1", TimeInterval(t1_run_start, t1_run_end), "RUNNING"
        ),
        VisualElement(
            "inv-1", "runner-1", TimeInterval(t1_paused, t1_paused), "PAUSED"
        ),
        VisualElement("inv-2", "runner-1", TimeInterval(t2_start, t2_end), "RUNNING"),
        VisualElement(
            "inv-1", "runner-1", TimeInterval(t1_resume_start, t1_resume_end), "RESUMED"
        ),
    ]

    result = assign_lanes(elements)

    lane_inv1_run = result[ElementLaneKey("inv-1", "runner-1", t1_run_start)]
    lane_inv2 = result[ElementLaneKey("inv-2", "runner-1", t2_start)]
    lane_inv1_resume = result[ElementLaneKey("inv-1", "runner-1", t1_resume_start)]

    assert lane_inv1_run is not None
    assert lane_inv2 is not None
    assert lane_inv1_resume is not None
