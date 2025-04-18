"""
Test for cron-based task triggering in the Redis trigger system.

This test verifies that tasks can be scheduled using cron expressions
and are executed at the appropriate times.
"""

import time

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
}

app = Pynenc(app_id="test_trigger_redis_cron", config_values=config)


# Using a faster cron expression for testing: every 15 seconds
# (using non-standard cron extension supported by croniter)
@app.task(triggers=TriggerBuilder().on_cron("*/15 * * * * *"))
def cron_task() -> float:
    """Return the current time."""
    return time.time()


@pytest.mark.timeout(90)
def test_on_cron_trigger(runner: None) -> None:
    """
    Test that a task is triggered based on a cron schedule.

    The test waits for the cron task to be triggered multiple times
    with the faster schedule (every 15 seconds) and verifies the timing.
    """
    max_wait_time = 60  # seconds
    poll_interval = 2  # seconds
    start_time = time.time()
    invocations = []

    # Wait for the cron trigger to fire, polling every few seconds
    while time.time() - start_time < max_wait_time:
        invocations = list(app.orchestrator.get_existing_invocations(cron_task))
        sorted_inv = sorted(invocations, key=lambda x: x.result)
        if len(invocations) > 2:
            break
        time.sleep(poll_interval)

    assert (
        len(invocations) > 1
    ), "Expected at least 2 invocations of cron_task within the wait time"

    time_diff = sorted_inv[-1].result - sorted_inv[-2].result
    assert time_diff < 20, "Cron task invocations should be less than 20 seconds apart"
    assert time_diff >= 10, "Cron task invocations should be at least 10 seconds apart"
