"""
Test for combined OR and AND logic trigger execution.

This is a comprehensive test that verifies both logic types work correctly
when triggered by the same source task execution.
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
app = Pynenc(app_id="test_combined_logic_focus", config_values=config)


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


def test_or_and_logic_together(runner: Any) -> None:
    """
    Test that OR and AND logic work correctly when both are present.

    This is a comprehensive test that verifies both logic types work correctly
    when triggered by the same source task execution.
    """
    # Execute source task to trigger both types
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

    # Verify AND logic triggers once
    and_invocations = list(
        app.orchestrator.get_existing_invocations(and_triggered_task)
    )
    assert (
        len(and_invocations) == 1
    ), f"AND logic should trigger once, got {len(and_invocations)}"

    # Verify results are properly formed
    or_results = [inv.result for inv in or_invocations]
    assert len(set(or_results)) == 2, "OR trigger results should be unique"

    for result in or_results:
        assert "or_triggered_" in result

    assert "and_triggered_" in and_invocations[0].result
