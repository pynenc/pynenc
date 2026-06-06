"""
Unit tests for trigger monitoring DTOs.

Covers ``EventRecord`` and ``TriggerRunRecord`` serialization, default
values, and basic property semantics.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any

from pynenc.trigger.monitoring import (
    EventRecord,
    TriggerRunRecord,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class _FakeDataStore:
    """Pass-through serializer that emulates the client data store API."""

    def serialize(self, value: Any, _flag: bool) -> Any:
        return value

    def deserialize(self, value: Any) -> Any:
        return value


class _FakeApp:
    """Minimal stand-in for ``Pynenc`` used in DTO serialization tests."""

    def __init__(self) -> None:
        self.client_data_store = _FakeDataStore()


def _fake_app() -> Pynenc:
    return _FakeApp()  # type: ignore[return-value]


def test_event_record_defaults() -> None:
    """An event record stores the basics and exposes matched/triggered flags."""
    record = EventRecord(event_id="evt-1", event_code="user.created")

    assert record.event_id == "evt-1"
    assert record.event_code == "user.created"
    assert record.payload == {}
    assert record.matched_condition_ids == []
    assert record.triggered_invocation_ids == []
    assert record.matched is False
    assert record.triggered is False
    assert record.timestamp.tzinfo is not None


def test_event_record_matched_and_triggered_flags() -> None:
    record = EventRecord(
        event_id="evt-2",
        event_code="x",
        matched_condition_ids=["c1"],
        triggered_invocation_ids=["inv-1"],
    )

    assert record.matched is True
    assert record.triggered is True


def test_event_record_json_round_trip() -> None:
    timestamp = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    record = EventRecord(
        event_id="evt-3",
        event_code="order.paid",
        payload={"amount": 42, "currency": "EUR"},
        timestamp=timestamp,
        matched_condition_ids=["cond-1"],
        valid_condition_ids=["vc-1"],
        triggered_invocation_ids=["inv-1"],
        emitted_by_invocation_id="inv-source",
        emitted_by_task_id="tasks.emit",
    )

    raw = record.to_json(_fake_app())
    restored = EventRecord.from_json(raw, _fake_app())

    assert restored == record


def test_trigger_run_record_json_round_trip() -> None:
    claimed = datetime(2026, 5, 12, 10, 0, tzinfo=UTC)
    executed = claimed + timedelta(seconds=1)
    record = TriggerRunRecord(
        trigger_run_id="run-1",
        trigger_id="trig-1",
        task_id_key="tasks.react",
        logic_value="and",
        valid_condition_ids=["vc-1", "vc-2"],
        condition_ids=["c-1", "c-2"],
        event_ids=["evt-1"],
        source_invocation_ids=["inv-source"],
        triggered_invocation_id="inv-result",
        arguments_preview={"x": 1},
        claimed_at=claimed,
        executed_at=executed,
    )

    restored = TriggerRunRecord.from_json(record.to_json())

    assert restored == record


def test_trigger_run_record_optional_timestamps_round_trip() -> None:
    record = TriggerRunRecord(
        trigger_run_id="run-2",
        trigger_id="trig-2",
        task_id_key="tasks.x",
        logic_value="or",
    )

    restored = TriggerRunRecord.from_json(record.to_json())

    assert restored.claimed_at is None
    assert restored.executed_at is None
    assert restored.event_ids == []
