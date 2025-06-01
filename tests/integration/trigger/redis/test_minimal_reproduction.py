"""
Minimal reproduction test to debug trigger issue.
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
app = Pynenc(app_id="test_minimal_reproduction", config_values=config)


@app.task
def source_task() -> str:
    """Source task that will trigger others."""
    return f"source_{datetime.now(timezone.utc).isoformat()}"


@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("or")
)
def or_trigger_task() -> str:
    """Task triggered by OR logic."""
    return f"or_triggered_{datetime.now(timezone.utc).isoformat()}"


@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("and")
)
def and_trigger_task() -> str:
    """Task triggered by AND logic."""
    return f"and_triggered_{datetime.now(timezone.utc).isoformat()}"


def test_minimal_or_and_logic(runner: Any) -> None:
    """
    Minimal test that should work exactly like the working test.
    """
    # Execute source task
    source_invocation = source_task()
    result = source_invocation.result
    assert result is not None
    print(f"Source task completed with result: {result}")

    # Give trigger system time to process
    time.sleep(2)

    # Check triggered tasks
    or_invocations = list(app.orchestrator.get_existing_invocations(or_trigger_task))
    and_invocations = list(app.orchestrator.get_existing_invocations(and_trigger_task))

    print(f"OR invocations: {len(or_invocations)}")
    print(f"AND invocations: {len(and_invocations)}")

    # These should match the working test expectations
    assert (
        len(or_invocations) == 2
    ), f"OR should trigger twice, got {len(or_invocations)}"
    assert (
        len(and_invocations) == 1
    ), f"AND should trigger once, got {len(and_invocations)}"
