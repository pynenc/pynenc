"""
Simplified comprehensive test for trigger behavior with OR/AND logic.

This test verifies that the fix for OR logic in trigger_loop_iteration()
works correctly without the complexity of multiple module-level tasks.
"""

import time
from datetime import datetime, timezone
from typing import Any

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
app = Pynenc(app_id="test_comprehensive_trigger_simple", config_values=config)


# Define tasks at module level to work with runner fixture
@app.task
def source_task() -> str:
    """Source task that triggers others."""
    return f"source_{datetime.now(timezone.utc).isoformat()}"


@app.task
def source_failure() -> None:
    """Source task that fails."""
    raise RuntimeError("Test failure")


# OR logic trigger - should execute twice (once for status, once for result)
@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("or")
)
def or_triggered_task() -> str:
    """Task triggered by OR logic."""
    return f"or_triggered_{datetime.now(timezone.utc).isoformat()}"


# AND logic trigger - should execute once (when both status AND result are met)
@app.task(
    triggers=TriggerBuilder()
    .on_status(source_task, statuses=["SUCCESS"])
    .on_any_result(source_task)
    .with_logic("and")
)
def and_triggered_task() -> str:
    """Task triggered by AND logic."""
    return f"and_triggered_{datetime.now(timezone.utc).isoformat()}"


# Single condition trigger - baseline test
@app.task(triggers=TriggerBuilder().on_status(source_task, statuses=["SUCCESS"]))
def single_triggered_task() -> str:
    """Task triggered by single condition."""
    return f"single_triggered_{datetime.now(timezone.utc).isoformat()}"


# Exception trigger
@app.task(triggers=TriggerBuilder().on_exception(source_failure))
def exception_triggered_task() -> str:
    """Task triggered by exception."""
    return f"exception_triggered_{datetime.now(timezone.utc).isoformat()}"


def test_or_and_logic_comprehensive(runner: Any) -> None:
    """
    Test that OR logic triggers multiple times and AND logic triggers once.

    This is a simplified version of the comprehensive test that avoids
    creating separate app instances and uses the module-level app.
    """

    # Execute source task to trigger others
    success_invocation = source_task()
    result = success_invocation.result
    assert result is not None

    # Execute failure task
    with pytest.raises(RuntimeError):
        failure_invocation = source_failure()
        _ = failure_invocation.result

    # Give trigger system time to process
    time.sleep(4)

    # Verify OR logic: should trigger twice
    or_invocations = list(app.orchestrator.get_existing_invocations(or_triggered_task))
    assert (
        len(or_invocations) == 2
    ), f"OR logic should trigger twice, got {len(or_invocations)}"

    # Verify AND logic: should trigger once
    and_invocations = list(
        app.orchestrator.get_existing_invocations(and_triggered_task)
    )
    assert (
        len(and_invocations) == 1
    ), f"AND logic should trigger once, got {len(and_invocations)}"

    # Verify single condition: should trigger once
    single_invocations = list(
        app.orchestrator.get_existing_invocations(single_triggered_task)
    )
    assert (
        len(single_invocations) == 1
    ), f"Single condition should trigger once, got {len(single_invocations)}"

    # Verify exception trigger: should trigger once
    exception_invocations = list(
        app.orchestrator.get_existing_invocations(exception_triggered_task)
    )
    assert (
        len(exception_invocations) == 1
    ), f"Exception trigger should trigger once, got {len(exception_invocations)}"

    # Verify all results are unique and contain expected patterns
    or_results = [inv.result for inv in or_invocations]
    assert len(set(or_results)) == 2, "OR trigger results should be unique"
    for result in or_results:
        assert "or_triggered_" in result

    assert "and_triggered_" in and_invocations[0].result
    assert "single_triggered_" in single_invocations[0].result
    assert "exception_triggered_" in exception_invocations[0].result


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
