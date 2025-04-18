"""
Context for event-based conditions.

This module defines the EventContext class, which provides the necessary data
for evaluating event-based trigger conditions. It includes the event ID and
the associated payload.
"""

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, ClassVar

from pynenc.trigger.arguments import ArgumentFilter
from pynenc.trigger.conditions.base import ConditionContext, TriggerCondition

if TYPE_CHECKING:
    from ...app import Pynenc


@dataclass
class EventContext(ConditionContext):
    """
    Context for event-based conditions.

    Contains the necessary information about an event that occurred in the system,
    allowing event-based conditions to determine if they should trigger.
    """

    event_id: str  # Unique identifier for this event instance
    event_code: str  # Type of event (used for matching conditions)
    payload: dict[str, Any] = field(default_factory=dict)  # Event data

    @property
    def context_id(self) -> str:
        return f"event_{self.event_code}_{self.event_id}"

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this event context.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        data: dict = {
            "event_id": self.event_id,
            "event_code": self.event_code,
        }

        # Serialize payload if present
        if self.payload:
            serialized_payload = {}
            for key, value in self.payload.items():
                serialized_payload[key] = app.arg_cache.serialize(value, False)
            data["payload"] = serialized_payload

        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "EventContext":
        """
        Create an EventContext from parsed JSON data.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new EventContext instance
        """
        event_id = data.get("event_id")
        event_code = data.get("event_code")

        if not event_id or not event_code:
            raise ValueError("Missing required fields in EventContext data")

        payload_data = data.get("payload", {})

        payload = {}
        if payload_data:
            for key, value in payload_data.items():
                payload[key] = app.arg_cache.deserialize(value)
        else:
            payload = payload_data

        return cls(event_id=event_id, event_code=event_code, payload=payload)


class EventCondition(TriggerCondition[EventContext]):
    """
    Condition based on system events.

    Triggers a task when a specific event occurs, optionally filtered by event parameters.
    """

    # Set the expected context type
    context_type: ClassVar[type[EventContext]] = EventContext

    def __init__(self, event_code: str, payload_filter: ArgumentFilter):
        """
        Create an event-based trigger condition.

        :param event_code: Type of event this condition matches
        :param required_params: Optional parameters that must exist with matching values in the event
        """
        self.event_code = event_code
        self.payload_filter = payload_filter

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this event condition.

        :return: A string ID based on the event code and required parameters
        """
        return f"event#{self.event_code}#{self.payload_filter.filter_id}"

    def get_source_task_ids(self) -> set[str]:
        return set()

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        return {
            "event_code": self.event_code,
            "payload_filter": self.payload_filter.to_json(app),
        }

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "EventCondition":
        """
        Create an EventCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new EventCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        event_code = data.get("event_code")
        if not event_code:
            raise ValueError("Missing required event_code in EventCondition data")
        payload_filter = ArgumentFilter.from_json(data["payload_filter"], app)
        return cls(event_code=event_code, payload_filter=payload_filter)

    def _is_satisfied_by(self, context: EventContext) -> bool:
        """
        Check if this condition is satisfied by the given event context.

        For an event condition to be satisfied:
        1. The event code must match
        2. All required parameters must exist in the payload with matching values

        :param context: Event context to evaluate
        :return: True if the condition is satisfied by the event
        """
        # Check if event code matches
        if context.event_code != self.event_code:
            return False

        # Check if all required parameters exist with matching values
        return self.payload_filter.filter_arguments(context.payload)

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        Event conditions aren't directly tied to specific tasks.

        :param task_id: ID of the task to check
        :return: Always False for event conditions
        """
        return False
