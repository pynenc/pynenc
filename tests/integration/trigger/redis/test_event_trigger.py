"""
Test for event-based task triggering in the Redis trigger system.

This test verifies that tasks can be triggered by events and
are executed with the correct payload data.
"""

import time
from typing import Any

from pynenc import Pynenc
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

app = Pynenc(app_id="test_trigger_redis_event", config_values=config)


@app.task
def emit_event(event_code: str, payload: dict[str, Any] | None = None) -> str:
    """
    Emit an event with the given code and payload.

    :param event_code: The event code to emit
    :param payload: Optional payload data for the event
    :return: The event ID of the emitted event
    """
    return app.trigger.emit_event(event_code, payload or {})


@app.task(
    triggers=TriggerBuilder()
    .on_event("test_event", payload_filter={"x": 5, "y": 7})
    .with_arguments({"x": 5, "y": 7})
)
def add_on_event(x: int, y: int) -> int:
    """Task that runs when a specific event is emitted."""
    return x + y


def test_on_event_trigger(runner: None) -> None:
    """
    Test that a task is triggered when an event is emitted.

    Verifies that the add_on_event task (which has an event trigger)
    is executed when the specified event is emitted.
    """
    # Emit the event that should trigger the task
    event_code = "test_event"
    payload = {"x": 5, "y": 7}
    event_invocation = emit_event(event_code, payload)

    # Wait for event to be processed and trigger to execute
    time.sleep(1)

    # Check that add_on_event was executed
    invocations = list(app.orchestrator.get_existing_invocations(add_on_event))

    # Should have at least one invocation
    assert len(invocations) > 0

    # Find the invocation triggered by our event (should be the most recent)
    triggered_invocation = invocations[-1]

    # Verify it has correct arguments
    assert triggered_invocation.call.arguments.kwargs == payload

    # Verify it was triggered after our event was emitted
    assert triggered_invocation.identity.invocation_id != event_invocation.invocation_id
