import uuid
from datetime import datetime, timezone
from typing import Any


class EventDefinition:
    """Represents a defined event type in the system."""

    name: str
    description: str | None
    schema: dict[str, Any] | None  # JSON schema for payload validation

    def __init__(
        self,
        name: str,
        description: str | None = None,
        schema: dict[str, Any] | None = None,
    ) -> None:
        self.name = name
        self.description = description
        self.schema = schema


class EventInstance:
    """Represents a specific occurrence of an event."""

    event_id: str
    event_code: str
    payload: dict[str, Any]
    timestamp: datetime

    def __init__(
        self,
        event_code: str,
        payload: dict[str, Any],
        event_id: str | None = None,
        timestamp: datetime | None = None,
    ) -> None:
        self.event_id = event_id or str(uuid.uuid4())
        self.event_code = event_code
        self.payload = payload
        self.timestamp = timestamp or datetime.now(timezone.utc)
