"""Unit tests for trigger event definition and instance models."""

from datetime import UTC, datetime
from uuid import UUID

from pynenc.trigger.trigger_events import EventDefinition, EventInstance


def test_event_definition_stores_event_metadata() -> None:
    """Event definitions keep the public event contract in one object."""
    schema = {
        "type": "object",
        "properties": {"source": {"type": "string"}},
        "required": ["source"],
    }

    definition = EventDefinition(
        "feed.updated",
        description="Feed refresh completed",
        schema=schema,
    )

    assert definition.name == "feed.updated"
    assert definition.description == "Feed refresh completed"
    assert definition.schema == schema


def test_event_instance_generates_uuid_and_aware_timestamp() -> None:
    """Runtime events get a unique id and UTC-aware timestamp by default."""
    before = datetime.now(UTC)
    event = EventInstance("feed.updated", {"source": "rss"})
    after = datetime.now(UTC)

    uuid_value = UUID(event.event_id)
    assert str(uuid_value) == event.event_id
    assert event.event_code == "feed.updated"
    assert event.payload == {"source": "rss"}
    assert event.timestamp.tzinfo is not None
    assert before <= event.timestamp <= after


def test_event_instance_preserves_supplied_identity_and_timestamp() -> None:
    """Replay/deserialization paths can pass durable event identity explicitly."""
    timestamp = datetime(2026, 6, 6, 12, 30, tzinfo=UTC)
    payload = {"count": 3}

    event = EventInstance(
        "article.ingested",
        payload,
        event_id="evt-existing",
        timestamp=timestamp,
    )

    assert event.event_id == "evt-existing"
    assert event.event_code == "article.ingested"
    assert event.payload == payload
    assert event.timestamp == timestamp
