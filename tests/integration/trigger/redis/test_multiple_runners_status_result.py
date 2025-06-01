"""
Test for status and result triggers with multiple runners in Redis trigger system.

This test verifies that when multiple runners are active simultaneously,
tasks triggered by status changes and results are executed exactly once
rather than multiple times.
"""

import time
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


def test_multiple_runners_on_task_end(runner: None) -> None:
    """
    Test that status-triggered task executes exactly once with multiple runners.

    This test uses the runner fixture to properly start the trigger system,
    then simulates multiple workers by running tasks concurrently to ensure
    that triggered tasks are executed exactly once despite multiple runners.
    """
    # Execute source tasks concurrently to simulate multiple runners
    source_invocation = source_task()

    # Wait for source task to complete
    assert source_invocation.result is not None

    # Execute exception task
    with pytest.raises(RuntimeError):
        exception_invocation = source_exception_task()
        _ = exception_invocation.result

    # Give trigger system time to process the status/result changes
    time.sleep(2)

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
    assert (
        len(status_invocations) == 1
    ), f"Expected 1 status invocation, got {len(status_invocations)}"
    assert (
        len(result_invocations) == 1
    ), f"Expected 1 result invocation, got {len(result_invocations)}"
    assert (
        len(combined_or_invocations) == 2
    ), f"Combined OR should trigger twice, got {len(combined_or_invocations)}"
    assert (
        len(combined_and_invocations) == 1
    ), f"Combined AND should trigger once, got {len(combined_and_invocations)}"
    assert (
        len(exception_invocations) == 1
    ), f"Expected 1 exception invocation, got {len(exception_invocations)}"
