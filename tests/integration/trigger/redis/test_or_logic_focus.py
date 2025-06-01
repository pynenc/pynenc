"""
Test for OR logic trigger execution - verifies multiple executions.

This test focuses specifically on verifying that the OR logic fix works correctly.
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
app = Pynenc(app_id="test_or_logic_focus", config_values=config)


# Source task
@app.task
def source_task() -> str:
    """Source task that generates both status and result conditions."""
    return f"source_{datetime.now(timezone.utc).isoformat()}"


# OR logic trigger - should execute twice (status + result)
@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("or")
)
def or_triggered_task() -> str:
    """Task triggered by OR logic."""
    return f"or_triggered_{datetime.now(timezone.utc).isoformat()}"


def test_or_logic_multiple_execution(runner: Any) -> None:
    """
    Test that OR logic triggers execute multiple times when multiple conditions are met.

    This verifies that the fix for OR logic in trigger_loop_iteration() works correctly.
    """
    # Execute source task to trigger both status and result conditions
    invocation = source_task()
    result = invocation.result
    assert result is not None

    # Give trigger system time to process
    time.sleep(4)

    # Verify OR logic triggers twice
    or_invocations = list(app.orchestrator.get_existing_invocations(or_triggered_task))
    assert (
        len(or_invocations) == 2
    ), f"OR logic should trigger twice, got {len(or_invocations)}"

    # Verify all results are unique
    or_results = [inv.result for inv in or_invocations]
    assert len(set(or_results)) == 2, "OR trigger results should be unique"

    # Verify results contain expected pattern
    for result in or_results:
        assert "or_triggered_" in result
