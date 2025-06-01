"""
Test to understand AND logic semantics with different tasks.
"""

import time
from datetime import datetime, timezone
from typing import Any

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
app = Pynenc(app_id="test_and_logic_semantics", config_values=config)


@app.task
def task_a() -> str:
    """First task."""
    return "result_a"


@app.task
def task_b() -> int:
    """Second task that returns a specific value."""
    return 42


@app.task(
    triggers=TriggerBuilder()
    .on_any_result(task_a)
    .on_result(task_b, filter_result=42)
    .with_logic("and")
)
def and_different_tasks() -> str:
    """Triggered when both different tasks complete."""
    return f"and_different_{datetime.now(timezone.utc).isoformat()}"


@app.task(
    triggers=TriggerBuilder()
    .on_any_result(task_a)
    .on_result(task_b, filter_result=42)
    .with_logic("or")
)
def or_different_tasks() -> str:
    """Triggered when either task completes."""
    return f"or_different_{datetime.now(timezone.utc).isoformat()}"


def test_and_or_different_tasks(runner: Any) -> None:
    """
    Test AND/OR logic with conditions on different tasks.
    """
    # Execute both tasks
    invocation_a = task_a()
    result_a = invocation_a.result
    assert result_a == "result_a"

    invocation_b = task_b()
    result_b = invocation_b.result
    assert result_b == 42

    # Give trigger system time to process
    time.sleep(2)

    # Check triggered tasks
    and_invocations = list(
        app.orchestrator.get_existing_invocations(and_different_tasks)
    )
    or_invocations = list(app.orchestrator.get_existing_invocations(or_different_tasks))

    print(f"AND invocations: {len(and_invocations)}")
    print(f"OR invocations: {len(or_invocations)}")

    # With separate task executions, AND should still trigger once if both conditions are met
    # OR should trigger twice (once per task completion)
    assert (
        len(and_invocations) == 1
    ), f"AND should trigger once when both conditions are met, got {len(and_invocations)}"
    assert (
        len(or_invocations) == 2
    ), f"OR should trigger twice (once per task), got {len(or_invocations)}"
