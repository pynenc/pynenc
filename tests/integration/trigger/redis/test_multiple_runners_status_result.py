"""
Test for status and result triggers with multiple runners in Redis trigger system.

This test verifies that when multiple runners are active simultaneously,
tasks triggered by status changes and results are executed exactly once
rather than multiple times.
"""

import threading
import time
from collections.abc import Generator
from datetime import datetime, timezone

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
}
app = Pynenc(
    app_id="test_trigger_redis_multi_runner_status_result", config_values=config
)


@app.task
def source_task() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.task
def source_exception_task() -> None:
    raise RuntimeError("This is a test exception")


@app.task(triggers=TriggerBuilder().on_status(source_task, statuses=["SUCCESS"]))
def status_triggered_task() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.task(triggers=TriggerBuilder().on_any_result(source_task))
def result_triggered_task() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("or")
)
def combined_or_trigger_task() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("and")
)
def combined_and_trigger_task() -> str:
    return datetime.now(timezone.utc).isoformat()


@app.task(triggers=TriggerBuilder().on_exception(source_exception_task))
def exception_triggered_task() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    app.purge()
    yield
    app.purge()


def test_multiple_runners_on_task_end(setup_teardown: None) -> None:
    """
    Test that status-triggered task executes exactly once with multiple runners.

    This test creates multiple concurrent runners and ensures that the
    status-triggered task is executed exactly once for each source task run.
    """
    # Start multiple runners
    worker_threads = []
    for _ in range(5):
        worker_thread = threading.Thread(target=run_worker, daemon=True)
        worker_threads.append(worker_thread)
        worker_thread.start()

    # Wait for source task result
    _ = source_task().result
    with pytest.raises(RuntimeError):
        _ = source_exception_task().result

    # Let workers time to process conditions on previous task
    time.sleep(10)

    # Wait for all worker threads to finish
    for thread in worker_threads:
        thread.join(timeout=3)

    # Get invocations from the orchestrator
    status_invocations = list(
        app.orchestrator.get_existing_invocations(status_triggered_task)
    )
    result_invocations = list(
        app.orchestrator.get_existing_invocations(result_triggered_task)
    )
    combined_or_invocations = list(
        app.orchestrator.get_existing_invocations(combined_or_trigger_task)
    )
    combined_and_invocations = list(
        app.orchestrator.get_existing_invocations(combined_and_trigger_task)
    )
    exception_invocations = list(
        app.orchestrator.get_existing_invocations(exception_triggered_task)
    )

    # Verify expected length of invocations
    assert len(status_invocations) == 1
    assert len(result_invocations) == 1
    assert len(combined_or_invocations) == 2, "Combined OR should trigger twice"
    assert len(combined_and_invocations) == 1, "Combined AND should trigger once"
    assert len(exception_invocations) == 1
