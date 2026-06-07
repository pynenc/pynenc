"""JSON projections for trigger-monitoring DTOs.

These helpers are used by FastAPI routers to render :mod:`pynenc.trigger`
records as plain dicts in API responses without leaking dataclass details
into templates. They are shared between the events and invocations views.
"""

from __future__ import annotations

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.trigger.monitoring import (
        EventMarker,
        EventMarkerPage,
        EventRecord,
        TriggerRunParticipant,
        TriggerRunRecord,
    )


def relevant_trigger_run_participants(
    run: TriggerRunRecord,
) -> list[TriggerRunParticipant]:
    """Return the participants that form the run's coherent trigger cause."""
    participants = list(run.participants)
    if str(run.logic_value).lower() != "and" or len(run.condition_ids) < 2:
        return participants

    by_source: dict[str, list[TriggerRunParticipant]] = {}
    for participant in participants:
        if participant.source_invocation_id:
            by_source.setdefault(participant.source_invocation_id, []).append(
                participant
            )
    coherent_sources = {
        source_id: source_participants
        for source_id, source_participants in by_source.items()
        if len({p.condition_id for p in source_participants if p.condition_id}) > 1
    }
    if not coherent_sources:
        return participants

    source_id, source_participants = max(
        coherent_sources.items(),
        key=lambda item: (
            len({p.condition_id for p in item[1] if p.condition_id}),
            max(
                (
                    p.context_timestamp.timestamp()
                    for p in item[1]
                    if p.context_timestamp
                ),
                default=float("-inf"),
            ),
            item[0],
        ),
    )
    correlated_condition_ids = {
        p.condition_id for p in source_participants if p.condition_id
    }
    return [
        participant
        for participant in participants
        if participant.condition_id not in correlated_condition_ids
        or participant.source_invocation_id == source_id
    ]


def event_to_dict(event: EventRecord) -> dict:
    """Return a JSON-serializable dict for one :class:`EventRecord`."""
    return {
        "event_id": event.event_id,
        "event_code": event.event_code,
        "timestamp": event.timestamp.isoformat(),
        "matched": event.matched,
        "triggered": event.triggered,
        "matched_condition_ids": list(event.matched_condition_ids),
        "valid_condition_ids": list(event.valid_condition_ids),
        "triggered_invocation_ids": list(event.triggered_invocation_ids),
        "emitted_by_invocation_id": event.emitted_by_invocation_id,
        "emitted_by_task_id": event.emitted_by_task_id,
        "emitted_by_runner_context_id": event.emitted_by_runner_context_id,
        "payload": event.payload,
    }


def marker_to_dict(marker: EventMarker) -> dict:
    """Return a JSON-serializable dict for one :class:`EventMarker`."""
    return {
        "event_id": marker.event_id,
        "event_code": marker.event_code,
        "timestamp": marker.timestamp.isoformat(),
        "matched": marker.matched,
        "triggered": marker.triggered,
        "emitted_by_invocation_id": marker.emitted_by_invocation_id,
    }


def marker_page_to_dict(page: EventMarkerPage) -> dict:
    """Return a JSON-serializable dict for one :class:`EventMarkerPage`."""
    return {
        "markers": [marker_to_dict(m) for m in page.markers],
        "total": page.total,
        "truncated": page.truncated,
    }


def participant_to_dict(participant: TriggerRunParticipant) -> dict:
    """Return a JSON-serializable dict for one :class:`TriggerRunParticipant`."""
    ts = participant.context_timestamp
    return {
        "context_type": participant.context_type,
        "condition_id": participant.condition_id,
        "valid_condition_id": participant.valid_condition_id,
        "event_id": participant.event_id,
        "source_invocation_id": participant.source_invocation_id,
        "context_timestamp": ts.isoformat() if ts else None,
        "context_summary": participant.context_summary or "",
    }


def trigger_run_to_dict(run: TriggerRunRecord) -> dict:
    """Return a JSON-serializable dict for one :class:`TriggerRunRecord`.

    Includes the per-condition ``participants`` list so the UI can render
    a relation breakdown without reissuing per-condition lookups.
    """
    claimed = run.claimed_at
    executed = run.executed_at
    participants = relevant_trigger_run_participants(run)
    return {
        "trigger_run_id": run.trigger_run_id,
        "trigger_id": run.trigger_id,
        "task_id_key": run.task_id_key,
        "logic_value": run.logic_value,
        "valid_condition_ids": list(run.valid_condition_ids),
        "condition_ids": list(run.condition_ids),
        "event_ids": list(run.event_ids),
        "source_invocation_ids": list(run.source_invocation_ids),
        "triggered_invocation_id": run.triggered_invocation_id,
        "claimed_at": claimed.isoformat() if claimed else None,
        "executed_at": executed.isoformat() if executed else None,
        "atomic_service_run_id": run.atomic_service_run_id,
        "atomic_service_runner_id": run.atomic_service_runner_id,
        "participants": [participant_to_dict(p) for p in participants],
    }
