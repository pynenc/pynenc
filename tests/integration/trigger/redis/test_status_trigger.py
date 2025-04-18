"""
Test for status-based task triggering in the Redis trigger system.

This test verifies that tasks can be triggered when another task
reaches a specific execution status with matching arguments.
"""

import time
from typing import Any

from pynenc import Pynenc
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.conditions.status import StatusContext
from pynenc.trigger.trigger_builder import TriggerBuilder

# Configure app for testing
config = {
    "state_backend_cls": "RedisStateBackend",
    "serializer_cls": "JsonSerializer",
    "orchestrator_cls": "RedisOrchestrator",
    "broker_cls": "RedisBroker",
    "runner_cls": "ThreadRunner",
    "trigger_cls": "RedisTrigger",
}

app = Pynenc(app_id="test_trigger_redis_status", config_values=config)


@app.task
def add(x: int, y: int) -> int:
    """Add two numbers together."""
    return x + y


def build_args_from_status(s: StatusContext) -> dict[str, Any]:
    """
    Build arguments from the status context.

    :param s: The status context
    :return: A dictionary of arguments
    """
    return {"details": f"add called with {s.arguments.kwargs}"}


@app.task(
    triggers=TriggerBuilder()
    .on_status(add, InvocationStatus.SUCCESS, call_arguments={"x": 3, "y": 4})
    .with_args_from_status(build_args_from_status)
)
def report_after_add(details: str) -> str:
    """Task that runs after add task completes successfully."""
    return details


def test_on_status_trigger(runner: None) -> None:
    """
    Test that a task is triggered when another task reaches a specific status.

    Verifies that report_after_add task is executed when the add task
    completes successfully with specific arguments.
    """
    # Execute the first task with the arguments that should trigger report_after_add
    add_args = {"x": 3, "y": 4}
    add_invocation = add(**add_args)

    # Wait for it to complete and trigger the next task
    time.sleep(0.5)

    # Verify the result of the first task
    assert add_invocation.result == 7

    # Check that report_after_add was triggered
    invocations = list(app.orchestrator.get_existing_invocations(report_after_add))

    # Should have exactly one invocation
    assert len(invocations) == 1

    # Verify the most recent invocation has the expected arguments
    report_args = {"details": f"add called with {add_args}"}
    triggered_invocation = invocations[-1]
    assert triggered_invocation.call.arguments.kwargs == report_args
