"""
Tests for atomic service coordination logic.

Key components tested:
- Runner position calculation in ordered list
- Time slot calculation for distributed execution
- Time slot membership checking
- Overall service execution decision logic
- Execution time validation
"""

from datetime import UTC, datetime
from time import sleep, time
from typing import TYPE_CHECKING

from pynenc.orchestrator import atomic_service
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc


def create_runner_context(runner_id: str) -> RunnerContext:
    """Create a test runner context."""
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
    )


def create_active_runner_info(
    runner_id: str,
    creation_offset: float = 0.0,
    last_service_duration: float | None = None,
) -> atomic_service.ActiveRunnerInfo:
    """Create an ActiveRunnerInfo for testing."""
    current = time()

    last_service_start = None
    last_service_end = None
    if last_service_duration is not None:
        last_service_start = datetime.fromtimestamp(current - 60, tz=UTC)
        last_service_end = datetime.fromtimestamp(
            current - 60 + last_service_duration, tz=UTC
        )

    return atomic_service.ActiveRunnerInfo(
        runner_id=runner_id,
        creation_time=datetime.fromtimestamp(current + creation_offset, tz=UTC),
        last_heartbeat=datetime.fromtimestamp(current, tz=UTC),
        last_service_start=last_service_start,
        last_service_end=last_service_end,
    )


def test_calculate_runner_position_should_return_correct_index_when_runner_exists() -> (
    None
):
    """Test that runner position is correctly calculated."""
    runner1 = create_active_runner_info("runner-1", -2.0)
    runner2 = create_active_runner_info("runner-2", -1.0)
    runner3 = create_active_runner_info("runner-3", 0.0)
    active_runners = [runner1, runner2, runner3]

    position = atomic_service.calculate_runner_position(
        runner2.runner_id, active_runners
    )

    assert position == 1


def test_calculate_runner_position_should_return_none_when_runner_not_found() -> None:
    """Test that None is returned when runner is not in the list."""
    runner1 = create_active_runner_info("runner-1")
    runner2 = create_active_runner_info("runner-2")
    active_runners = [runner1, runner2]

    unknown_runner = create_runner_context("unknown-runner")
    position = atomic_service.calculate_runner_position(
        unknown_runner.runner_id, active_runners
    )

    assert position is None


def test_calculate_time_slot_should_divide_interval_equally_when_multiple_runners() -> (
    None
):
    """Test that time slots are calculated correctly for multiple runners."""
    # 3 runners, 6 minute interval, 1 minute margin
    # Should give: 2 min - 1 min = 1 min slots (60 seconds)
    start, end = atomic_service.calculate_time_slot(
        runner_position=1,
        total_runners=3,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
    )

    # Runner 1 should have slot [120, 180) seconds
    assert start == 120.0
    assert end == 180.0


def test_calculate_time_slot_should_handle_small_margin_when_margin_exceeds_slot() -> (
    None
):
    """Test that time slot calculation handles cases where margin is too large."""
    # 3 runners, 3 minute interval, 2 minute margin
    # Would give negative slot, should fallback to half the slot size
    start, end = atomic_service.calculate_time_slot(
        runner_position=0,
        total_runners=3,
        service_interval_minutes=3.0,
        spread_margin_minutes=2.0,
    )

    # Should have a valid window (half of 60 seconds = 30 seconds)
    assert start == 0.0
    assert end == 30.0
    assert end > start


def test_is_runner_in_time_slot_should_return_true_when_time_in_slot() -> None:
    """Test that time slot membership is correctly detected."""
    # Current time maps to 150 seconds in a 360-second cycle
    # Slot is [120, 180)
    current_time = 150.0
    service_interval = 6.0  # 360 seconds
    start_time = 120.0
    end_time = 180.0

    result = atomic_service.is_runner_in_time_slot(
        current_time, service_interval, start_time, end_time
    )

    assert result is True


def test_is_runner_in_time_slot_should_return_false_when_time_outside_slot() -> None:
    """Test that time outside slot is correctly detected."""
    # Current time maps to 90 seconds in a 360-second cycle
    # Slot is [120, 180)
    current_time = 90.0
    service_interval = 6.0  # 360 seconds
    start_time = 120.0
    end_time = 180.0

    result = atomic_service.is_runner_in_time_slot(
        current_time, service_interval, start_time, end_time
    )

    assert result is False


def test_is_runner_in_time_slot_should_handle_wraparound_in_cycle() -> None:
    """Test time slot checking across cycle boundaries."""
    # Current time is 370, which wraps to 10 in a 360-second cycle
    # Slot is [0, 60)
    current_time = 370.0
    service_interval = 6.0  # 360 seconds
    start_time = 0.0
    end_time = 60.0

    result = atomic_service.is_runner_in_time_slot(
        current_time, service_interval, start_time, end_time
    )

    assert result is True


def test_should_run_atomic_service_should_return_true_when_single_runner() -> None:
    """Test that single runner always runs services."""
    runner = create_active_runner_info("runner-1")
    active_runners = [runner]

    result = atomic_service.can_run_atomic_service(
        runner_id=runner.runner_id,
        active_runners=active_runners,
        current_time=time(),
        service_interval_minutes=5.0,
        spread_margin_minutes=1.0,
    )

    assert result is True


def test_should_run_atomic_service_should_return_false_when_no_runners() -> None:
    """Test that no runners means no service execution."""
    runner = create_runner_context("runner-1")
    active_runners: list[atomic_service.ActiveRunnerInfo] = []

    result = atomic_service.can_run_atomic_service(
        runner_id=runner.runner_id,
        active_runners=active_runners,
        current_time=time(),
        service_interval_minutes=5.0,
        spread_margin_minutes=1.0,
    )

    assert result is False


def test_should_run_atomic_service_should_distribute_execution_when_multiple_runners() -> (
    None
):
    """Test that multiple runners get non-overlapping time slots."""
    runner1 = create_active_runner_info("runner-1", -2.0)
    runner2 = create_active_runner_info("runner-2", -1.0)
    runner3 = create_active_runner_info("runner-3", 0.0)
    active_runners = [runner1, runner2, runner3]

    # 6 minute interval = 360 seconds
    # 3 runners = 120 second slots each
    # 1 minute margin = 60 second spread
    # Actual slots: [0,60), [120,180), [240,300)

    # Test at time 0 (runner1's slot)
    result1 = atomic_service.can_run_atomic_service(
        runner_id=runner1.runner_id,
        active_runners=active_runners,
        current_time=0.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
    )

    # Test at time 150 (runner2's slot)
    result2 = atomic_service.can_run_atomic_service(
        runner_id=runner2.runner_id,
        active_runners=active_runners,
        current_time=150.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
    )

    # Test at time 250 (runner3's slot)
    result3 = atomic_service.can_run_atomic_service(
        runner_id=runner3.runner_id,
        active_runners=active_runners,
        current_time=250.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
    )

    # Test runner1 at time 150 (not their slot)
    result1_wrong_time = atomic_service.can_run_atomic_service(
        runner_id=runner1.runner_id,
        active_runners=active_runners,
        current_time=150.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
    )

    assert result1 is True
    assert result2 is True
    assert result3 is True
    assert result1_wrong_time is False


def test_get_last_execution_duration_should_return_duration_when_both_timestamps_present() -> (
    None
):
    """Test that execution duration is correctly calculated."""
    runner = create_active_runner_info("runner-1", last_service_duration=45.5)

    duration = runner.get_last_execution_duration_seconds()

    assert duration is not None
    assert abs(duration - 45.5) < 0.1


def test_get_last_execution_duration_should_return_none_when_no_execution() -> None:
    """Test that None is returned when no execution recorded."""
    runner = create_active_runner_info("runner-1")

    duration = runner.get_last_execution_duration_seconds()

    assert duration is None


def test_get_max_execution_duration_should_return_maximum_across_runners() -> None:
    """Test that maximum execution duration is found across all runners."""
    runner1 = create_active_runner_info("runner-1", last_service_duration=10.0)
    runner2 = create_active_runner_info("runner-2", last_service_duration=25.5)
    runner3 = create_active_runner_info("runner-3")  # No execution history
    active_runners = [runner1, runner2, runner3]

    max_duration = atomic_service.get_max_execution_duration(active_runners)

    assert abs(max_duration - 25.5) < 0.1


def test_get_max_execution_duration_should_return_zero_when_no_history() -> None:
    """Test that zero is returned when no runners have execution history."""
    runner1 = create_active_runner_info("runner-1")
    runner2 = create_active_runner_info("runner-2")
    active_runners = [runner1, runner2]

    max_duration = atomic_service.get_max_execution_duration(active_runners)

    assert max_duration == 0.0


def test_sqlite_orchestrator_can_run_atomic_service_filter(
    app_instance: "Pynenc",
) -> None:
    """Test that register_runner_heartbeat and get_active_runners filter by can_run_atomic_service."""
    orchestrator = app_instance.orchestrator

    ctx_true = create_runner_context("runner-true")
    ctx_false = create_runner_context("runner-false")
    orchestrator.register_runner_heartbeats(
        [ctx_true.runner_id], can_run_atomic_service=True
    )
    orchestrator.register_runner_heartbeats(
        [ctx_false.runner_id], can_run_atomic_service=False
    )

    # Only runner with can_run_atomic_service=True
    runners_true = orchestrator.get_active_runners(can_run_atomic_service=True)
    assert len(runners_true) == 1
    assert runners_true[0].runner_id == ctx_true.runner_id

    # Only runner with can_run_atomic_service=False
    runners_false = orchestrator.get_active_runners(can_run_atomic_service=False)
    assert len(runners_false) == 1
    assert runners_false[0].runner_id == ctx_false.runner_id

    # All runners
    runners_all = orchestrator.get_active_runners(can_run_atomic_service=None)
    ids_all = {r.runner_id for r in runners_all}
    assert {ctx_true.runner_id, ctx_false.runner_id} == ids_all


def test_should_run_atomic_service_should_respect_time_slots_with_multiple_runners(
    app_instance: "Pynenc",
) -> None:
    """Test that atomic service scheduling assigns different time slots to runners."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    app_instance.conf.atomic_service_interval_minutes = 100.0

    try:
        # Mock time to be at the start of the cycle
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            runner1 = create_runner_context("runner-1")
            runner2 = create_runner_context("runner-2")

            app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
            sleep(0.01)
            app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])

            # At time=0, runner1 should be scheduled, runner2 should not
            should_run_1 = app_instance.orchestrator.should_run_atomic_service(runner1)
            should_run_2 = app_instance.orchestrator.should_run_atomic_service(runner2)

            assert should_run_1 is True, "Runner 1 should be scheduled at time=0"
            assert should_run_2 is False, "Runner 2 should NOT be scheduled at time=0"
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval


def test_should_run_atomic_service_should_handle_runner_cycling(
    app_instance: "Pynenc",
) -> None:
    """Test that runners get scheduled in rotation over time."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")
        runner3 = create_runner_context("runner-3")

        app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats([runner3.runner_id])

        # Test at different points in the cycle
        # 60min interval / 3 runners = 20min slots each
        # Runner1: [0-19min), Runner2: [20-39min), Runner3: [40-59min)

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0):
            assert app_instance.orchestrator.should_run_atomic_service(runner1) is True
            assert app_instance.orchestrator.should_run_atomic_service(runner2) is False
            assert app_instance.orchestrator.should_run_atomic_service(runner3) is False

        with patch(
            "pynenc.orchestrator.base_orchestrator.time", return_value=1200
        ):  # 20min
            assert app_instance.orchestrator.should_run_atomic_service(runner1) is False
            assert app_instance.orchestrator.should_run_atomic_service(runner2) is True
            assert app_instance.orchestrator.should_run_atomic_service(runner3) is False

        with patch(
            "pynenc.orchestrator.base_orchestrator.time", return_value=2400
        ):  # 40min
            assert app_instance.orchestrator.should_run_atomic_service(runner1) is False
            assert app_instance.orchestrator.should_run_atomic_service(runner2) is False
            assert app_instance.orchestrator.should_run_atomic_service(runner3) is True
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval


def test_record_atomic_service_execution_should_update_timestamps(
    app_instance: "Pynenc",
) -> None:
    """Test that atomic service execution timestamps are recorded."""
    runner_ctx = create_runner_context("test-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    start_time = datetime.now(UTC)
    sleep(0.01)
    end_time = datetime.now(UTC)

    app_instance.orchestrator.record_atomic_service_execution(
        runner_ctx.runner_id, start_time, end_time
    )

    active_runners = app_instance.orchestrator.get_active_runners(
        can_run_atomic_service=None
    )
    assert len(active_runners) == 1

    runner_info = active_runners[0]
    assert runner_info.last_service_start is not None
    assert runner_info.last_service_end is not None
    # Compare datetime objects directly
    assert abs((runner_info.last_service_start - start_time).total_seconds()) < 0.001
    assert abs((runner_info.last_service_end - end_time).total_seconds()) < 0.001
