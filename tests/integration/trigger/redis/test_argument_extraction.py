"""
Test for argument extraction in the Redis trigger system.

This test verifies that trigger systems can properly extract arguments
from event payloads and use them to invoke tasks.
"""

import time
from typing import Any

from pynenc import Pynenc
from pynenc.trigger.conditions.event import EventContext
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

app = Pynenc(app_id="test_trigger_redis_args_extract", config_values=config)


def get_args_from_event(event_context: EventContext) -> dict[str, Any]:
    """
    Extract arguments from the event context.

    :param event_context: The event context
    :return: A dictionary of arguments
    """
    return {
        "x": event_context.payload.get("num_0"),
        "y": event_context.payload.get("num_1"),
    }


@app.task(
    triggers=TriggerBuilder()
    .on_event("args_extraction_test")
    .with_args_from_event(get_args_from_event)
)
def add_with_extracted_args(x: int, y: int) -> int:
    """Task that extracts arguments from the event payload."""
    return x + y


def test_argument_extraction(runner: None) -> None:
    """
    Test extraction of arguments from event payload.

    Verifies that a trigger can extract arguments from
    the event payload that triggered it.
    """
    # Define event details
    event_code = "args_extraction_test"
    payload = {"num_0": 30, "num_1": 12, "extra": "ignored"}

    # Emit the event
    app.trigger.emit_event(event_code, payload)

    # Wait for event processing and task execution
    time.sleep(1)

    # Check if the task was triggered with the extracted arguments
    invocations = list(
        app.orchestrator.get_existing_invocations(add_with_extracted_args)
    )

    # Should have triggered 1 invocation
    assert len(invocations) == 1

    # Verify the most recent invocation has the extracted arguments
    triggered_invocation = invocations[-1]
    assert triggered_invocation.call.arguments.kwargs["x"] == payload["num_0"]
    assert triggered_invocation.call.arguments.kwargs["y"] == payload["num_1"]
    assert "extra" not in triggered_invocation.call.arguments.kwargs
