"""
Test for AND logic trigger execution - verifies single execution.

This test verifies that the OR logic fix doesn't break AND logic behavior.
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
app = Pynenc(app_id="test_and_logic_focus", config_values=config)


# Source task
@app.task
def source_task() -> str:
    """Source task that generates both status and result conditions."""
    return f"source_{datetime.now(timezone.utc).isoformat()}"


# AND logic trigger - should execute once (status AND result)
@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("and")
)
def and_triggered_task() -> str:
    """Task triggered by AND logic."""
    return f"and_triggered_{datetime.now(timezone.utc).isoformat()}"


def test_and_logic_single_execution(runner: Any) -> None:
    """
    Test that AND logic triggers execute only once when all conditions are met.

    This verifies that the OR logic fix doesn't break AND logic behavior.
    """
    # Execute source task to trigger both status and result conditions
    invocation = source_task()
    result = invocation.result
    assert result is not None

    # Give trigger system time to process
    time.sleep(4)

    # Verify AND logic triggers once
    and_invocations = list(
        app.orchestrator.get_existing_invocations(and_triggered_task)
    )
    assert (
        len(and_invocations) == 1
    ), f"AND logic should trigger once, got {len(and_invocations)}"

    # Verify result contains expected pattern
    assert "and_triggered_" in and_invocations[0].result
