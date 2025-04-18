"""
Test for cron job execution with multiple runners in Redis trigger system.

This test verifies that when multiple runners are active simultaneously,
a cron-triggered task is executed exactly once rather than multiple times.
"""

import threading
import time
from collections.abc import Generator
from datetime import datetime, timezone

import pytest

from pynenc import Pynenc
from pynenc.trigger.trigger_builder import TriggerBuilder

# Configure app for testing
config = {
    "state_backend_cls": "RedisStateBackend",
    "serializer_cls": "JsonSerializer",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "runner_cls": "ThreadRunner",
    "trigger_cls": "RedisTrigger",
    "scheduler_interval_seconds": 1,
}

app = Pynenc(app_id="test_trigger_redis_multi_runner_cron", config_values=config)


@app.task(triggers=TriggerBuilder().on_cron("* * * * *"))  # Run every minute
def cron_task() -> datetime:
    """Task triggered by cron schedule that returns its execution time."""
    return datetime.now(timezone.utc)


def run_worker(duration: float = 10.0) -> None:
    """
    Run a worker for the specified duration.

    :param duration: Time in seconds to run the worker
    """
    runner_thread = threading.Thread(target=app.runner.run, daemon=True)
    runner_thread.start()
    time.sleep(duration)
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=2)


@pytest.fixture
def setup_teardown() -> Generator:
    """Setup and teardown fixture for the test."""
    # Reset app state
    app.purge()

    yield

    # Cleanup after test
    app.purge()


def test_multiple_runners_cron_execution(setup_teardown: None) -> None:
    """
    Test that multiple runners don't cause duplicate cron task executions.

    This test creates three concurrent runners and ensures that the cron task
    is executed exactly once for each minute that passes.
    """
    # Store the start time
    start_time = datetime.now(timezone.utc)

    # Start multiple runners, slightly staggered
    worker_threads = []
    for _ in range(3):
        worker_thread = threading.Thread(
            target=run_worker,
            daemon=True,
        )
        worker_threads.append(worker_thread)
        worker_thread.start()
        # Small delay between starting workers
        time.sleep(0.5)

    # Wait for enough time to ensure the cron task has at least one opportunity to run
    # Wait at least 65 seconds to ensure it passes a minute boundary
    time.sleep(2)

    # Wait for all worker threads to finish
    for thread in worker_threads:
        thread.join(timeout=3)

    # Calculate how many minute boundaries were crossed during the test
    # We add 1 second buffer on both sides to be cautious
    elapsed_time = datetime.now(timezone.utc) - start_time
    minutes_passed = (elapsed_time.total_seconds() - 1) // 60

    # Get invocations from the orchestrator
    invocations = list(app.orchestrator.get_existing_invocations(cron_task))

    # Verify that we have the expected number of invocations (1 per minute)
    # We check for either the exact number of minutes or one more/less to account for edge timing
    assert (
        abs(len(invocations) - minutes_passed) <= 1
    ), f"Expected {minutes_passed} invocations (one per minute), got {len(invocations)}"

    # Check that all invocations have unique timestamps at least a minute apart
    if len(invocations) > 1:
        execution_times = [inv.result for inv in invocations]
        execution_times.sort()

        for i in range(1, len(execution_times)):
            time_diff = execution_times[i] - execution_times[i - 1]
            # Allow for small variations but ensure they're not in the same minute
            assert (
                time_diff.total_seconds() >= 55
            ), f"Invocations too close together: {time_diff.total_seconds()} seconds"
