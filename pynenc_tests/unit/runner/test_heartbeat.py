"""
Tests for heartbeat interval calculation logic and heartbeat registration.

Key components tested:
- Heartbeat interval calculation to prevent false dead runner detection
- Safe heartbeat intervals ensure 3+ heartbeats per timeout period
- Race condition prevention at timeout boundaries
- Immediate heartbeat registration on thread start (prevents recovery race condition)
"""

from typing import TYPE_CHECKING, Any

from pynenc.runner.heartbeat import (
    calculate_heartbeat_interval,
    start_invocation_runner_heartbeat,
)
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc


# ################################################################################### #
# HEARTBEAT INTERVAL CALCULATION TESTS
# ################################################################################### #


def test_calculate_heartbeat_interval_returns_seconds() -> None:
    """Test that the function returns interval in seconds."""
    # 0.5 minutes check, 10 minutes timeout
    result = calculate_heartbeat_interval(0.5, 10.0)

    # Should be less than 10 minutes (600 seconds) / 3 = 200 seconds
    assert result < 200
    assert isinstance(result, float)


def test_calculate_heartbeat_interval_respects_three_beat_minimum() -> None:
    """Test that heartbeat interval allows at least 3 beats per timeout."""
    timeout_minutes = 10.0
    check_interval_minutes = 0.5

    result = calculate_heartbeat_interval(check_interval_minutes, timeout_minutes)
    timeout_seconds = timeout_minutes * 60

    # Should be at most 1/3 of timeout
    assert result <= timeout_seconds / 3

    # Verify 3 heartbeats fit within timeout
    three_beats = result * 3
    assert three_beats <= timeout_seconds


def test_calculate_heartbeat_interval_uses_smaller_of_two_values() -> None:
    """Test that the smaller interval is always chosen."""
    # Case 1: Check interval is smaller
    check_small = 0.1  # 6 seconds
    timeout = 10.0  # 600 seconds, divided by 3 = 200 seconds
    result = calculate_heartbeat_interval(check_small, timeout)
    assert result == 6.0  # Converted to seconds

    # Case 2: Timeout/3 is smaller
    check_large = 1.0  # 60 seconds
    timeout = 2.0  # 120 seconds, divided by 3 = 40 seconds
    result = calculate_heartbeat_interval(check_large, timeout)
    assert result == 40.0  # Capped at timeout/3


def test_calculate_heartbeat_interval_prevents_race_condition() -> None:
    """
    Test that with equal check_interval and timeout, heartbeat is safe.

    This is the critical race condition: when timing values are equal or very close,
    we must ensure the heartbeat updates before timeout check happens.
    """
    equal_minutes = 3.0  # Same value for both

    result = calculate_heartbeat_interval(equal_minutes, equal_minutes)

    # Should be 1/3 of timeout to allow 3 heartbeats
    expected = (equal_minutes * 60) / 3
    assert result == expected

    # Verify math: 3 heartbeats should fit in 1 timeout
    assert result * 3 <= equal_minutes * 60


def test_calculate_heartbeat_interval_with_very_small_values() -> None:
    """Test calculation with very small timeout values (edge case)."""
    result = calculate_heartbeat_interval(0.05, 0.05)

    # 0.05 minutes = 3 seconds, divided by 3 = 1 second
    expected = 1.0
    assert result == expected

    # Verify 3 heartbeats fit
    assert result * 3 <= 3.0


def test_calculate_heartbeat_interval_with_large_values() -> None:
    """Test calculation with large timeout values."""
    result = calculate_heartbeat_interval(10.0, 60.0)

    # 60 minutes = 3600 seconds, divided by 3 = 1200 seconds
    # But check_interval is 600 seconds, so should use 600
    expected = 600.0
    assert result == expected


# ################################################################################### #
# RACE CONDITION PREVENTION TESTS
# ################################################################################### #


def test_heartbeat_at_timeout_boundary_prevented() -> None:
    """
    Test that heartbeat interval calculation prevents timeout boundary issues.

    If check_interval = timeout, we'd have a race condition.
    The function should return timeout/3 instead.
    """
    timeout = 10.0  # 10 minutes
    check = 10.0  # 10 minutes (problematic)

    result = calculate_heartbeat_interval(check, timeout)

    # Should be limited to 1/3 of timeout
    assert result == (10.0 * 60) / 3
    assert result < (10.0 * 60)


def test_three_heartbeats_always_fit_before_timeout() -> None:
    """
    Test that we can always fit at least 3 heartbeats within the timeout.

    This safety margin prevents race conditions: even if one heartbeat is missed,
    we still have 2 others to prevent false dead runner detection.
    """
    test_cases = [
        (0.1, 1.0),  # Small values
        (0.5, 10.0),  # Normal case
        (1.0, 60.0),  # Large values
        (0.05, 0.05),  # Edge case: equal values
    ]

    for check_min, timeout_min in test_cases:
        heartbeat_interval = calculate_heartbeat_interval(check_min, timeout_min)
        timeout_sec = timeout_min * 60

        # 3 heartbeats should fit in one timeout period
        three_heartbeats = heartbeat_interval * 3
        assert three_heartbeats <= timeout_sec, (
            f"Check={check_min}min, Timeout={timeout_min}min: "
            f"3 heartbeats ({three_heartbeats}s) don't fit in timeout ({timeout_sec}s)"
        )


# ################################################################################### #
# IMMEDIATE HEARTBEAT REGISTRATION TESTS (prevents recovery race condition)
# ################################################################################### #


def test_start_invocation_runner_heartbeat_registers_immediately(
    app_instance: "Pynenc",
) -> None:
    """
    Test that start_invocation_runner_heartbeat registers heartbeat BEFORE returning.

    This is critical to prevent a race condition where:
    1. Worker calls start_invocation_runner_heartbeat()
    2. Function returns (thread starts but hasn't sent heartbeat yet)
    3. Worker starts processing invocation
    4. Recovery service runs, sees no heartbeat for worker
    5. Recovery marks the RUNNING invocation as dead (FALSE POSITIVE!)

    The fix: register first heartbeat synchronously before returning.
    """
    runner_ctx = RunnerContext(
        runner_cls="PPRWorker",
        runner_id="test-immediate-heartbeat",
        pid=12345,
        hostname="test-host",
        extra_data={},
    )

    # Start heartbeat thread
    start_invocation_runner_heartbeat(app_instance, runner_ctx)

    # IMMEDIATELY check if heartbeat was registered (no sleep!)
    # This should pass if heartbeat is registered synchronously
    active_runners = app_instance.orchestrator._get_active_runners(
        timeout_seconds=60.0, can_run_atomic_service=False
    )
    runner_ids = {r.runner_ctx.runner_id for r in active_runners}

    assert runner_ctx.runner_id in runner_ids, (
        "Heartbeat must be registered IMMEDIATELY when start_invocation_runner_heartbeat() "
        "returns, not after the background thread's first iteration. "
        "This prevents recovery from marking the worker as dead before first heartbeat."
    )

    # Cleanup: thread is daemon so it will be cleaned up automatically


def test_first_heartbeat_is_synchronous_not_in_thread(
    app_instance: "Pynenc",
) -> None:
    """
    Verify that the first heartbeat is sent synchronously from the main thread.

    This test checks the thread identity of the first heartbeat call.
    If it's called from a background thread, this indicates a race condition:
    the function returns before the heartbeat is guaranteed to be registered.

    Production bug: heartbeat only sent inside thread loop creates a race where
    recovery can run before first heartbeat is registered.
    """
    from unittest.mock import patch
    import threading

    runner_ctx = RunnerContext(
        runner_cls="PPRWorker",
        runner_id="test-sync-heartbeat",
        pid=12345,
        hostname="test-host",
        extra_data={},
    )

    main_thread_id = threading.current_thread().ident
    first_call_thread_id: list = [None]
    original_register = app_instance.orchestrator.register_runner_heartbeat

    def track_calling_thread(*args: Any, **kwargs: Any) -> None:
        if first_call_thread_id[0] is None:
            first_call_thread_id[0] = threading.current_thread().ident
        return original_register(*args, **kwargs)

    with patch.object(
        app_instance.orchestrator,
        "register_runner_heartbeat",
        side_effect=track_calling_thread,
    ):
        start_invocation_runner_heartbeat(app_instance, runner_ctx)

        # Wait a moment for thread to potentially call register (if sync call didn't happen)
        import time

        time.sleep(0.01)

    assert first_call_thread_id[0] is not None, (
        "register_runner_heartbeat was never called"
    )

    assert first_call_thread_id[0] == main_thread_id, (
        f"First heartbeat was called from background thread (id={first_call_thread_id[0]}) "
        f"instead of main thread (id={main_thread_id}). "
        "The first heartbeat MUST be registered synchronously from the main thread "
        "before start_invocation_runner_heartbeat returns. Otherwise there's a race "
        "condition where recovery can mark the invocation as dead before the "
        "background thread sends its first heartbeat."
    )
