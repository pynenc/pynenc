"""
Test for cron-based task triggering in the Redis trigger system.

This test verifies that tasks can be scheduled using cron expressions
and are executed at the appropriate times.
"""

import time
from datetime import datetime

import pytest

from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

config = {
    "state_backend_cls": "RedisStateBackend",
    "serializer_cls": "JsonSerializer",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "runner_cls": "ThreadRunner",
    "trigger_cls": "RedisTrigger",
    # Using a faster schedule interval for testing
    "scheduler_interval_seconds": 2,
    "runner_loop_sleep_time_sec": 0.2,
}

app = Pynenc(app_id="test_trigger_redis_cron", config_values=config)


# Using a minute-level cron expression (fastest allowed precision)
# Running every minute to ensure test completes within a reasonable time
@app.task(triggers=TriggerBuilder().on_cron("* * * * *"))
def cron_task() -> float:
    """Return the current time."""
    return time.time()


@pytest.mark.timeout(150)
def test_on_cron_trigger(runner: None) -> None:
    """
    Test that a task is triggered based on a cron schedule.

    The test waits for the cron task to be triggered multiple times
    with a minute-level schedule and verifies the timing.
    """
    max_wait_time = (
        120  # seconds (enough for at least 2 executions at 1 minute intervals)
    )
    poll_interval = 5  # seconds
    start_time = time.time()

    # First, ensure any previous test data is cleared
    list(app.orchestrator.get_existing_invocations(cron_task))

    print(f"Starting test at {datetime.fromtimestamp(start_time)}")
    invocations = []

    # Wait for invocations to accumulate
    while time.time() - start_time < max_wait_time:
        invocations = list(app.orchestrator.get_existing_invocations(cron_task))
        print(f"Found {len(invocations)} invocations")

        # Break early if we have enough invocations
        if len(invocations) >= 2:
            break

        time.sleep(poll_interval)

    # Verify we have at least 2 invocations to check timing
    assert len(invocations) >= 2, "Expected at least 2 cron invocations"

    # Sort by result timestamp
    sorted_inv = sorted(invocations, key=lambda x: x.result)

    # Print invocation timestamps for debugging
    for i, inv in enumerate(sorted_inv):
        result_time = datetime.fromtimestamp(inv.result)
        print(f"Invocation {i+1}: {result_time.isoformat()}")

    # Calculate interval between invocations
    if len(sorted_inv) >= 2:
        # Check timing between last two invocations
        time_diff = sorted_inv[-1].result - sorted_inv[-2].result
        print(f"Time difference between last two invocations: {time_diff:.2f} seconds")

        # With * * * * * cron expression (every minute), allow 50-70 second range
        # to account for scheduling delays and check window
        assert (
            50 <= time_diff <= 70
        ), f"Expected interval of ~60s, but got {time_diff:.2f}s"
