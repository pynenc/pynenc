"""
Debug test to understand why the runner fixture isn't working in comprehensive tests.
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
app = Pynenc(app_id="test_debug_runner_fixture", config_values=config)


@app.task
def source_task() -> str:
    """Source task that will trigger others."""
    return f"source_{datetime.now(timezone.utc).isoformat()}"


@app.task(triggers=TriggerBuilder().on_status(source_task, statuses=["SUCCESS"]))
def triggered_task() -> str:
    """Task triggered by source task success."""
    return f"triggered_{datetime.now(timezone.utc).isoformat()}"


def test_runner_fixture_debug(runner: Any) -> None:
    """
    Debug test to check if runner fixture works.
    """
    # Check if runner fixture was applied
    print(f"Runner fixture value: {runner}")

    # Execute source task
    invocation = source_task()
    result = invocation.result
    assert result is not None
    print(f"Source task result: {result}")

    # Give trigger system time to process
    time.sleep(2)

    # Check if triggered task was executed
    triggered_invocations = list(
        app.orchestrator.get_existing_invocations(triggered_task)
    )
    print(f"Number of triggered invocations: {len(triggered_invocations)}")

    assert (
        len(triggered_invocations) == 1
    ), f"Expected 1 triggered invocation, got {len(triggered_invocations)}"
