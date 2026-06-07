"""
Monitoring DTOs for the trigger component.

These dataclasses are the durable monitoring contract that trigger backends
expose to any visibility / observability consumer. They follow the same
shape as ``InvocationHistory`` and other pynenc DTOs: backend-neutral, JSON
round-trippable, UTC-aware timestamps, and no dependency on any specific UI.

Module contents:

- :class:`EventRecord`: durable record of one emitted event.
- :class:`EventMarker`: lightweight projection used by timeline overlays.
- :class:`EventMarkerPage`: paginated marker projection with truncation flag.
- :class:`TriggerRunRecord`: durable record of one trigger run.
- :class:`TriggerRunParticipant`: per-condition row attached to a trigger run
  preserving which condition each event or source invocation satisfied.

Design notes:

- ``EventRecord.triggered_invocation_ids`` is rehydrated on read from the
  per-event invocation index rather than rewritten on the hot path.
- :class:`TriggerRunParticipant` stores only reference ids and lightweight
  labels. Detail views re-fetch the live ``TriggerCondition`` / context
  through :meth:`BaseTrigger.get_condition` and the event / state-backend
  lookups when they need full payloads.
"""

from __future__ import annotations

import json
from dataclasses import dataclass, field
from datetime import UTC, datetime
from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from pynenc.app import Pynenc


def _now_utc() -> datetime:
    """Return a UTC timezone-aware ``datetime`` for ``default_factory`` usage."""
    return datetime.now(UTC)


# --------------------------------------------------------------------------- #
# Events
# --------------------------------------------------------------------------- #


@dataclass
class EventRecord:
    """
    Durable record of an emitted event.

    Created by :meth:`pynenc.trigger.base_trigger.BaseTrigger.emit_event`
    regardless of whether any condition matched. The first write captures the
    payload; a single follow-up write records matched/valid condition IDs
    when at least one condition accepts the event.

    ``triggered_invocation_ids`` is populated by the backend on read from a
    dedicated event->invocation index. The hot path does not rewrite the
    event JSON to append invocation IDs.
    """

    event_id: str
    event_code: str
    payload: dict[str, Any] = field(default_factory=dict)
    timestamp: datetime = field(default_factory=_now_utc)
    matched_condition_ids: list[str] = field(default_factory=list)
    valid_condition_ids: list[str] = field(default_factory=list)
    triggered_invocation_ids: list[str] = field(default_factory=list)
    emitted_by_invocation_id: str | None = None
    emitted_by_task_id: str | None = None
    emitted_by_runner_context_id: str | None = None

    @property
    def matched(self) -> bool:
        """True when at least one event condition accepted this event."""
        return bool(self.matched_condition_ids)

    @property
    def triggered(self) -> bool:
        """True when at least one invocation was created from this event."""
        return bool(self.triggered_invocation_ids)

    def to_json(self, app: Pynenc) -> str:
        """Serialize to JSON, routing payload values through the data store."""
        return json.dumps(self._to_dict(app))

    def _to_dict(self, app: Pynenc) -> dict[str, Any]:
        """Return a JSON-ready dict using ``app.client_data_store`` for payload."""
        payload_data: dict[str, Any] = {
            key: app.client_data_store.serialize(value, False)
            for key, value in self.payload.items()
        }
        return {
            "event_id": self.event_id,
            "event_code": self.event_code,
            "timestamp": self.timestamp.isoformat(),
            "payload": payload_data,
            "matched_condition_ids": list(self.matched_condition_ids),
            "valid_condition_ids": list(self.valid_condition_ids),
            "triggered_invocation_ids": list(self.triggered_invocation_ids),
            "emitted_by_invocation_id": self.emitted_by_invocation_id,
            "emitted_by_task_id": self.emitted_by_task_id,
            "emitted_by_runner_context_id": self.emitted_by_runner_context_id,
        }

    @classmethod
    def from_json(cls, json_str: str, app: Pynenc) -> EventRecord:
        """Rebuild an :class:`EventRecord` from its JSON representation."""
        return cls._from_dict(json.loads(json_str), app)

    @classmethod
    def _from_dict(cls, data: dict[str, Any], app: Pynenc) -> EventRecord:
        payload = {
            key: app.client_data_store.deserialize(value)
            for key, value in (data.get("payload") or {}).items()
        }
        emitted_by = data.get("emitted_by_invocation_id")
        return cls(
            event_id=data["event_id"],
            event_code=data["event_code"],
            payload=payload,
            timestamp=_parse_timestamp(data.get("timestamp")),
            matched_condition_ids=list(data.get("matched_condition_ids") or []),
            valid_condition_ids=list(data.get("valid_condition_ids") or []),
            triggered_invocation_ids=list(data.get("triggered_invocation_ids") or []),
            emitted_by_invocation_id=emitted_by,
            emitted_by_task_id=data.get("emitted_by_task_id"),
            emitted_by_runner_context_id=data.get("emitted_by_runner_context_id"),
        )


@dataclass(frozen=True)
class EventMarker:
    """
    Lightweight projection of an event used by the timeline overlay.

    Backends populate this directly from indexed columns without
    deserializing the full event payload. The marker is intentionally narrow
    so a busy timeline window can load many markers cheaply.
    """

    event_id: str
    event_code: str
    timestamp: datetime
    matched: bool
    triggered: bool
    emitted_by_invocation_id: str | None = None
    emitted_by_runner_context_id: str | None = None


@dataclass(frozen=True)
class EventMarkerPage:
    """
    Paginated event-marker projection with explicit truncation metadata.

    ``truncated`` is ``True`` when the backend stopped at ``limit`` and more
    markers exist for the requested window. Pynmon uses this to surface a
    visible "showing N of more" hint instead of silently dropping markers.
    """

    markers: list[EventMarker]
    total: int
    truncated: bool


# --------------------------------------------------------------------------- #
# Trigger runs
# --------------------------------------------------------------------------- #


@dataclass(frozen=True)
class TriggerRunParticipant:
    """
    Per-condition row attached to a :class:`TriggerRunRecord`.

    Preserves the mapping between a valid condition and the event or source
    invocation that satisfied it. Composite (AND) triggers need this to
    answer "which condition did each participant satisfy?".

    Exactly one of ``event_id`` / ``source_invocation_id`` is populated for
    event/status/result/exception conditions. ``CronContext`` and other
    contexts without a discrete source populate neither.

    ``context_timestamp`` and ``context_summary`` are lightweight labels so
    the timeline overlay and tooltip rows can anchor and describe a
    participant without re-fetching the source record. Full condition and
    context payloads are re-fetched on demand from the trigger registry and
    the event / state-backend lookups when a detail panel is opened.
    """

    context_type: str
    condition_id: str | None = None
    valid_condition_id: str | None = None
    event_id: str | None = None
    source_invocation_id: str | None = None
    context_timestamp: datetime | None = None
    context_summary: str = ""

    def to_dict(self) -> dict[str, Any]:
        return {
            "context_type": self.context_type,
            "condition_id": self.condition_id,
            "valid_condition_id": self.valid_condition_id,
            "event_id": self.event_id,
            "source_invocation_id": self.source_invocation_id,
            "context_timestamp": _iso_or_none(self.context_timestamp),
            "context_summary": self.context_summary,
        }

    @classmethod
    def from_dict(cls, data: dict[str, Any]) -> TriggerRunParticipant:
        return cls(
            context_type=data["context_type"],
            condition_id=data.get("condition_id"),
            valid_condition_id=data.get("valid_condition_id"),
            event_id=data.get("event_id"),
            source_invocation_id=data.get("source_invocation_id"),
            context_timestamp=_parse_timestamp_optional(data.get("context_timestamp")),
            context_summary=data.get("context_summary") or "",
        )


@dataclass
class TriggerRunRecord:
    """
    Durable record of a single trigger run.

    Captures the participants that satisfied the trigger and the resulting
    invocation. ``participants`` preserves the per-condition mapping that
    the parallel ``event_ids`` / ``source_invocation_ids`` lists lose. The
    parallel lists are kept for backward compatibility and as a flat index
    for backends that do not need the per-condition mapping.
    """

    trigger_run_id: str
    trigger_id: str
    task_id_key: str
    logic_value: str
    valid_condition_ids: list[str] = field(default_factory=list)
    condition_ids: list[str] = field(default_factory=list)
    event_ids: list[str] = field(default_factory=list)
    source_invocation_ids: list[str] = field(default_factory=list)
    triggered_invocation_id: str | None = None
    arguments_preview: dict[str, Any] = field(default_factory=dict)
    claimed_at: datetime | None = None
    executed_at: datetime | None = None
    participants: list[TriggerRunParticipant] = field(default_factory=list)
    # ID of the atomic-service execution cycle that produced this trigger run
    # (set when the trigger loop iteration runs inside ``_check_atomic_services``).
    # ``None`` for trigger runs produced outside the atomic-service path
    # (e.g. status / event / result-driven triggers from a task).
    atomic_service_run_id: str | None = None
    atomic_service_runner_id: str | None = None

    def to_json(self) -> str:
        """Serialize to JSON. Arguments preview is stored as plain JSON."""
        return json.dumps(
            {
                "trigger_run_id": self.trigger_run_id,
                "trigger_id": self.trigger_id,
                "task_id_key": self.task_id_key,
                "logic_value": self.logic_value,
                "valid_condition_ids": list(self.valid_condition_ids),
                "condition_ids": list(self.condition_ids),
                "event_ids": list(self.event_ids),
                "source_invocation_ids": list(self.source_invocation_ids),
                "triggered_invocation_id": self.triggered_invocation_id,
                "arguments_preview": self.arguments_preview,
                "claimed_at": _iso_or_none(self.claimed_at),
                "executed_at": _iso_or_none(self.executed_at),
                "participants": [p.to_dict() for p in self.participants],
                "atomic_service_run_id": self.atomic_service_run_id,
                "atomic_service_runner_id": self.atomic_service_runner_id,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> TriggerRunRecord:
        """Rebuild a :class:`TriggerRunRecord` from its JSON representation."""
        data = json.loads(json_str)
        return cls(
            trigger_run_id=data["trigger_run_id"],
            trigger_id=data["trigger_id"],
            task_id_key=data["task_id_key"],
            logic_value=data["logic_value"],
            valid_condition_ids=list(data.get("valid_condition_ids") or []),
            condition_ids=list(data.get("condition_ids") or []),
            event_ids=list(data.get("event_ids") or []),
            source_invocation_ids=list(data.get("source_invocation_ids") or []),
            triggered_invocation_id=data.get("triggered_invocation_id"),
            arguments_preview=dict(data.get("arguments_preview") or {}),
            claimed_at=_parse_timestamp_optional(data.get("claimed_at")),
            executed_at=_parse_timestamp_optional(data.get("executed_at")),
            participants=[
                TriggerRunParticipant.from_dict(p)
                for p in data.get("participants") or []
            ],
            atomic_service_run_id=data.get("atomic_service_run_id"),
            atomic_service_runner_id=data.get("atomic_service_runner_id"),
        )


# --------------------------------------------------------------------------- #
# Internal helpers
# --------------------------------------------------------------------------- #


def _parse_timestamp(value: Any) -> datetime:
    """Parse an ISO timestamp string, defaulting to "now" when missing."""
    if not value:
        return _now_utc()
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        parsed = parsed.replace(tzinfo=UTC)
    return parsed


def _parse_timestamp_optional(value: Any) -> datetime | None:
    """Parse an ISO timestamp string into UTC, returning ``None`` when missing."""
    if not value:
        return None
    return _parse_timestamp(value)


def _iso_or_none(value: datetime | None) -> str | None:
    """Return ``value.isoformat()`` or ``None``."""
    return value.isoformat() if value is not None else None
