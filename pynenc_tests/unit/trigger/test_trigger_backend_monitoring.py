"""
Backend contract tests for trigger event monitoring.

Validates ``store_event``, ``store_trigger_run``, and the related query
methods across ``MemTrigger`` and ``SQLiteTrigger`` via the ``app_instance``
parametrized fixture.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, Any, cast

import pytest

from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.trigger.monitoring import EventRecord, TriggerRunRecord
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.trigger import BaseTrigger


_module_app = MockPynenc()


def _test_atomic_service_run() -> AtomicServiceRun:
    return AtomicServiceRun(
        runner_id="test-runner",
        atomic_service_run_id="test-service-run",
    )


@_module_app.task
def trigger_target_task(x: int) -> int:
    return x + 1


def _make_event(
    event_id: str,
    *,
    code: str = "user.created",
    timestamp: datetime | None = None,
    matched: list[str] | None = None,
    triggered: list[str] | None = None,
) -> EventRecord:
    return EventRecord(
        event_id=event_id,
        event_code=code,
        timestamp=timestamp or datetime.now(UTC),
        payload={"k": "v"},
        matched_condition_ids=list(matched or []),
        triggered_invocation_ids=list(triggered or []),
    )


def _make_run(
    run_id: str,
    *,
    invocation_id: str = "inv-1",
    event_ids: list[str] | None = None,
    source_invocation_ids: list[str] | None = None,
    task_id_key: str = "tasks.x",
    executed_at: datetime | None = None,
) -> TriggerRunRecord:
    return TriggerRunRecord(
        trigger_run_id=run_id,
        trigger_id="trig-1",
        task_id_key=task_id_key,
        logic_value="and",
        valid_condition_ids=["vc-1"],
        condition_ids=["c-1"],
        event_ids=list(event_ids or []),
        source_invocation_ids=list(source_invocation_ids or []),
        triggered_invocation_id=invocation_id,
        claimed_at=datetime.now(UTC),
        executed_at=executed_at or datetime.now(UTC),
    )


@pytest.fixture
def trigger(app_instance: Pynenc) -> BaseTrigger:
    """Return the trigger backend from the parametrized app fixture."""
    return app_instance.trigger


def test_get_event_returns_none_when_unknown(trigger: BaseTrigger) -> None:
    assert trigger.get_event("missing") is None


def test_store_and_get_event_round_trip(trigger: BaseTrigger) -> None:
    event = _make_event("evt-1", matched=["c-1"], triggered=["inv-1"])
    trigger.store_event(event)

    fetched = trigger.get_event("evt-1")

    assert fetched is not None
    assert fetched.event_id == "evt-1"
    assert fetched.matched_condition_ids == ["c-1"]
    assert fetched.triggered_invocation_ids == ["inv-1"]


def test_get_events_filters_by_code_and_state(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("e1", code="a", matched=["c1"]))
    trigger.store_event(_make_event("e2", code="b"))
    trigger.store_event(_make_event("e3", code="a"))

    matched = trigger.get_events(matched=True)
    by_code = trigger.get_events(event_code="a")

    assert {r.event_id for r in matched} == {"e1"}
    assert {r.event_id for r in by_code} == {"e1", "e3"}


def test_get_events_orders_by_timestamp_descending(trigger: BaseTrigger) -> None:
    older = datetime.now(UTC) - timedelta(minutes=5)
    newer = datetime.now(UTC)
    trigger.store_event(_make_event("old", timestamp=older))
    trigger.store_event(_make_event("new", timestamp=newer))

    events = trigger.get_events()

    assert [e.event_id for e in events] == ["new", "old"]


def test_get_events_time_range(trigger: BaseTrigger) -> None:
    far_past = datetime.now(UTC) - timedelta(hours=2)
    recent = datetime.now(UTC) - timedelta(minutes=1)
    trigger.store_event(_make_event("old", timestamp=far_past))
    trigger.store_event(_make_event("recent", timestamp=recent))

    events = trigger.get_events(start_time=datetime.now(UTC) - timedelta(minutes=10))

    assert {e.event_id for e in events} == {"recent"}


def test_event_marker_preserves_emitter_runner_context(trigger: BaseTrigger) -> None:
    now = datetime.now(UTC)
    event = _make_event("runner-event", timestamp=now, triggered=["inv-1"])
    event.emitted_by_runner_context_id = "ExternalRunner@host-123"
    trigger.store_event(event)

    page = trigger.get_event_markers_in_timerange(
        now - timedelta(seconds=1),
        now + timedelta(seconds=1),
        state="triggered",
    )

    assert len(page.markers) == 1
    assert page.markers[0].emitted_by_runner_context_id == "ExternalRunner@host-123"


def test_count_events_matches_get_events_count(trigger: BaseTrigger) -> None:
    for i in range(3):
        trigger.store_event(_make_event(f"e{i}", code="x"))
    trigger.store_event(_make_event("other", code="y"))

    assert trigger.count_events(event_code="x") == 3
    assert trigger.count_events() == 4


def test_list_event_codes_returns_distinct_sorted(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("e1", code="b"))
    trigger.store_event(_make_event("e2", code="a"))
    trigger.store_event(_make_event("e3", code="a"))

    assert trigger.list_event_codes() == ["a", "b"]


def test_store_and_get_trigger_run(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("evt-1"))
    run = _make_run("run-1", event_ids=["evt-1"])
    trigger.store_trigger_run(run)

    fetched = trigger.get_trigger_run("run-1")

    assert fetched is not None
    assert fetched.trigger_run_id == "run-1"
    assert fetched.event_ids == ["evt-1"]


def test_get_trigger_runs_for_event(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("evt-1"))
    trigger.store_event(_make_event("evt-2"))
    trigger.store_trigger_run(_make_run("r1", event_ids=["evt-1"]))
    trigger.store_trigger_run(_make_run("r2", event_ids=["evt-2"]))

    runs = trigger.get_trigger_runs_for_event("evt-1")

    assert {r.trigger_run_id for r in runs} == {"r1"}


def test_get_trigger_runs_for_invocation(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("evt-1"))
    trigger.store_trigger_run(
        _make_run("r1", invocation_id="inv-1", event_ids=["evt-1"])
    )
    trigger.store_trigger_run(
        _make_run("r2", invocation_id="inv-2", event_ids=["evt-1"])
    )

    runs = trigger.get_trigger_runs_for_invocation("inv-1")

    assert {r.trigger_run_id for r in runs} == {"r1"}


def test_get_trigger_runs_in_timerange(trigger: BaseTrigger) -> None:
    now = datetime.now(UTC)
    trigger.store_event(_make_event("evt-1"))
    trigger.store_trigger_run(
        _make_run("r1", event_ids=["evt-1"], executed_at=now - timedelta(hours=2))
    )
    trigger.store_trigger_run(_make_run("r2", event_ids=["evt-1"], executed_at=now))

    runs = trigger.get_trigger_runs_in_timerange(
        now - timedelta(minutes=10), now + timedelta(minutes=1)
    )

    assert {r.trigger_run_id for r in runs} == {"r2"}


def test_purge_clears_monitoring_records(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("e1"))
    trigger.store_trigger_run(_make_run("r1", event_ids=["e1"]))

    trigger.purge()

    assert trigger.get_event("e1") is None
    assert trigger.get_trigger_run("r1") is None
    assert trigger.list_event_codes() == []


def test_emit_event_persists_unmatched_record(trigger: BaseTrigger) -> None:
    """Even with no matching condition the event is recorded."""
    event_id = trigger.emit_event("nobody_listens", {"k": "v"})

    record = trigger.get_event(event_id)

    assert record is not None
    assert record.event_code == "nobody_listens"
    assert record.payload == {"k": "v"}
    assert record.matched_condition_ids == []


def test_emit_event_records_matched_conditions(trigger: BaseTrigger) -> None:
    """A matching event condition shows up on the stored record."""
    from pynenc.trigger.arguments import create_argument_filter
    from pynenc.trigger.conditions import EventCondition

    cond = EventCondition("user.created", create_argument_filter({"k": "v"}))
    trigger.register_condition(cond)

    event_id = trigger.emit_event("user.created", {"k": "v"})

    record = trigger.get_event(event_id)
    assert record is not None
    assert record.matched_condition_ids == [cond.condition_id]


def test_trigger_loop_records_trigger_run_for_event(
    trigger: BaseTrigger,
) -> None:
    """End-to-end: emit_event + trigger loop produces a TriggerRunRecord."""
    from pynenc.trigger.arguments import (
        StaticArgumentProvider,
        create_argument_filter,
    )
    from pynenc.trigger.conditions import EventCondition
    from pynenc.trigger.trigger_definitions import TriggerDefinition

    trigger_target_task.app = trigger.app
    cond = EventCondition("user.created", create_argument_filter(None))
    trigger.register_condition(cond)
    tdef = TriggerDefinition(
        task_id=trigger_target_task.task_id,
        condition_ids=[cond.condition_id],
        argument_provider=StaticArgumentProvider({"x": 1}),
    )
    trigger.register_trigger(tdef.to_dto(trigger.app))

    event_id = trigger.emit_event("user.created", {"k": "v"})
    trigger.trigger_loop_iteration(_test_atomic_service_run())

    runs = trigger.get_trigger_runs_for_event(event_id)
    assert len(runs) == 1
    run = runs[0]
    assert run.task_id_key == trigger_target_task.task_id.key
    assert event_id in run.event_ids
    assert len(run.participants) == 1
    participant = run.participants[0]
    assert participant.context_type == "EventContext"
    assert participant.event_id == event_id
    record = trigger.get_event(event_id)
    assert record is not None
    assert run.triggered_invocation_id in record.triggered_invocation_ids


def test_emit_event_captures_emitter_invocation_id(
    trigger: BaseTrigger,
) -> None:
    """When invoked inside an active task, ``emitted_by_invocation_id`` is set."""
    from unittest.mock import MagicMock

    from pynenc import context

    fake_invocation = MagicMock()
    fake_invocation.invocation_id = "inv-123"
    context.swap_dist_invocation_context(trigger.app.app_id, fake_invocation)
    try:
        event_id = trigger.emit_event("user.created", {})
    finally:
        context.swap_dist_invocation_context(trigger.app.app_id, None)

    record = trigger.get_event(event_id)
    assert record is not None
    assert record.emitted_by_invocation_id == "inv-123"


def test_emit_event_outside_task_has_no_emitter_id(trigger: BaseTrigger) -> None:
    from pynenc import context

    context.clear_runner_context(trigger.app.app_id)
    context.clear_current_runner(trigger.app.app_id)
    event_id = trigger.emit_event("user.created", {})
    record = trigger.get_event(event_id)
    assert record is not None
    assert record.emitted_by_invocation_id is None
    assert record.emitted_by_runner_context_id is not None
    runner_context = trigger.app.state_backend.get_runner_context(
        record.emitted_by_runner_context_id
    )
    assert runner_context is not None
    assert runner_context.runner_cls == "ExternalRunner"


def test_emit_event_populates_valid_condition_ids(trigger: BaseTrigger) -> None:
    """Matched events must record both ``matched_`` and ``valid_condition_ids``.

    The two lists are produced from the same evaluation and should be aligned
    one-to-one for matched conditions.
    """
    from pynenc.trigger.arguments import create_argument_filter
    from pynenc.trigger.conditions import EventCondition

    cond = EventCondition("user.created", create_argument_filter({"k": "v"}))
    trigger.register_condition(cond)

    event_id = trigger.emit_event("user.created", {"k": "v"})

    record = trigger.get_event(event_id)
    assert record is not None
    assert record.matched_condition_ids == [cond.condition_id]
    assert len(record.valid_condition_ids) == len(record.matched_condition_ids)
    assert all(record.valid_condition_ids)


def test_get_events_emitted_by_invocation_filters_by_emitter(
    trigger: BaseTrigger,
) -> None:
    """The default helper returns only events emitted by the given invocation."""
    from unittest.mock import MagicMock

    from pynenc import context

    fake_invocation = MagicMock()
    fake_invocation.invocation_id = "inv-emit"
    context.swap_dist_invocation_context(trigger.app.app_id, fake_invocation)
    try:
        emitted = trigger.emit_event("user.created", {})
    finally:
        context.swap_dist_invocation_context(trigger.app.app_id, None)
    other = trigger.emit_event("user.created", {})

    matches = trigger.get_events_emitted_by_invocation("inv-emit")
    matched_ids = [e.event_id for e in matches]

    assert emitted in matched_ids
    assert other not in matched_ids


# ---------------------------------------------------------------------------
# Phase 4: trigger relation APIs (source-invocation + batch)
# ---------------------------------------------------------------------------


def test_get_trigger_runs_sourced_by_invocation_returns_matches(
    trigger: BaseTrigger,
) -> None:
    """Returns runs where the invocation appears as a source participant."""
    trigger.store_event(_make_event("e1"))
    trigger.store_trigger_run(
        _make_run(
            "r1",
            invocation_id="triggered-1",
            event_ids=["e1"],
            source_invocation_ids=["src-A"],
        )
    )
    trigger.store_trigger_run(
        _make_run(
            "r2",
            invocation_id="triggered-2",
            event_ids=["e1"],
            source_invocation_ids=["src-B"],
        )
    )
    trigger.store_trigger_run(
        _make_run(
            "r3",
            invocation_id="triggered-3",
            event_ids=["e1"],
            source_invocation_ids=["src-A", "src-B"],
        )
    )

    runs = trigger.get_trigger_runs_sourced_by_invocation("src-A")

    assert {r.trigger_run_id for r in runs} == {"r1", "r3"}


def test_get_trigger_runs_sourced_by_invocation_empty_when_unknown(
    trigger: BaseTrigger,
) -> None:
    trigger.store_event(_make_event("e1"))
    trigger.store_trigger_run(_make_run("r1", event_ids=["e1"]))

    assert trigger.get_trigger_runs_sourced_by_invocation("nobody") == []


def test_get_trigger_runs_sourced_by_invocation_is_inverse_of_triggered(
    trigger: BaseTrigger,
) -> None:
    """``triggered_invocation_id`` is not a source participant."""
    trigger.store_event(_make_event("e1"))
    trigger.store_trigger_run(
        _make_run(
            "r1",
            invocation_id="triggered-only",
            event_ids=["e1"],
            source_invocation_ids=[],
        )
    )

    # The triggered invocation should not show up as a source.
    assert trigger.get_trigger_runs_sourced_by_invocation("triggered-only") == []
    # But it does show up via the triggered-invocation lookup.
    assert {
        r.trigger_run_id
        for r in trigger.get_trigger_runs_for_invocation("triggered-only")
    } == {"r1"}


def test_get_events_batch_returns_mapping_with_missing_as_none(
    trigger: BaseTrigger,
) -> None:
    trigger.store_event(_make_event("e1"))
    trigger.store_event(_make_event("e2"))

    batch = trigger.get_events_batch(["e1", "e2", "missing"])

    assert set(batch.keys()) == {"e1", "e2", "missing"}
    assert batch["e1"] is not None and batch["e1"].event_id == "e1"
    assert batch["e2"] is not None and batch["e2"].event_id == "e2"
    assert batch["missing"] is None


def test_get_events_batch_handles_empty_input(trigger: BaseTrigger) -> None:
    assert trigger.get_events_batch([]) == {}


def test_get_trigger_runs_batch_returns_mapping(trigger: BaseTrigger) -> None:
    trigger.store_event(_make_event("e1"))
    trigger.store_trigger_run(_make_run("r1", event_ids=["e1"]))
    trigger.store_trigger_run(_make_run("r2", event_ids=["e1"]))

    batch = trigger.get_trigger_runs_batch(["r1", "r2", "missing"])

    assert set(batch.keys()) == {"r1", "r2", "missing"}
    assert batch["r1"] is not None
    assert batch["r2"] is not None
    assert batch["missing"] is None


def test_get_conditions_batch_returns_mapping(trigger: BaseTrigger) -> None:
    from pynenc.trigger.arguments import create_argument_filter
    from pynenc.trigger.conditions import EventCondition

    cond1 = EventCondition("user.created", create_argument_filter(None))
    cond2 = EventCondition("user.deleted", create_argument_filter(None))
    trigger.register_condition(cond1)
    trigger.register_condition(cond2)

    batch = trigger.get_conditions_batch(
        [cond1.condition_id, cond2.condition_id, "missing"]
    )

    assert set(batch.keys()) == {cond1.condition_id, cond2.condition_id, "missing"}
    assert batch[cond1.condition_id] is not None
    assert batch[cond2.condition_id] is not None
    assert batch["missing"] is None


def test_get_triggers_batch_returns_mapping(trigger: BaseTrigger) -> None:
    from pynenc.trigger.arguments import (
        StaticArgumentProvider,
        create_argument_filter,
    )
    from pynenc.trigger.conditions import EventCondition
    from pynenc.trigger.trigger_definitions import TriggerDefinition

    trigger_target_task.app = trigger.app
    cond = EventCondition("user.created", create_argument_filter(None))
    trigger.register_condition(cond)
    tdef = TriggerDefinition(
        task_id=trigger_target_task.task_id,
        condition_ids=[cond.condition_id],
        argument_provider=StaticArgumentProvider({"x": 1}),
    )
    trigger.register_trigger(tdef.to_dto(trigger.app))

    batch = trigger.get_triggers_batch([tdef.trigger_id, "missing"])

    assert set(batch.keys()) == {tdef.trigger_id, "missing"}
    assert batch[tdef.trigger_id] is not None
    assert batch["missing"] is None


def test_trigger_run_participants_capture_context_metadata(
    trigger: BaseTrigger,
) -> None:
    """End-to-end: participants record ``context_timestamp`` and summary."""
    from pynenc.trigger.arguments import (
        StaticArgumentProvider,
        create_argument_filter,
    )
    from pynenc.trigger.conditions import EventCondition
    from pynenc.trigger.trigger_definitions import TriggerDefinition

    trigger_target_task.app = trigger.app
    cond = EventCondition("user.created", create_argument_filter(None))
    trigger.register_condition(cond)
    tdef = TriggerDefinition(
        task_id=trigger_target_task.task_id,
        condition_ids=[cond.condition_id],
        argument_provider=StaticArgumentProvider({"x": 1}),
    )
    trigger.register_trigger(tdef.to_dto(trigger.app))

    event_id = trigger.emit_event("user.created", {"k": "v"})
    trigger.trigger_loop_iteration(_test_atomic_service_run())

    runs = trigger.get_trigger_runs_for_event(event_id)
    assert len(runs) == 1
    run = runs[0]
    assert run.participants, "expected at least one participant"
    p = run.participants[0]
    assert p.context_type == "EventContext"
    assert p.event_id == event_id
    assert p.context_timestamp is not None
    # ``context_extra_tokens`` for EventContext emits the event code token
    # (the event_id itself is captured separately in ``event_id``).
    assert p.context_summary == "code:user.created"


def test_trigger_run_record_keeps_only_coherent_composite_participants(
    trigger: BaseTrigger,
) -> None:
    """Stale contexts for one AND condition must not leak into monitoring."""
    from unittest.mock import Mock

    from pynenc.invocation.status import InvocationStatus
    from pynenc.trigger.arguments import create_argument_filter, create_result_filter
    from pynenc.trigger.conditions import (
        ResultCondition,
        ResultContext,
        StatusCondition,
        StatusContext,
        ValidCondition,
    )
    from pynenc.trigger.trigger_context import TriggerContext
    from pynenc.trigger.trigger_definitions import TriggerDefinition

    trigger_target_task.app = trigger.app
    older = cast("DistributedInvocation[Any, Any]", trigger_target_task(1))
    matching = cast("DistributedInvocation[Any, Any]", trigger_target_task(8))
    status_condition = StatusCondition(
        trigger_target_task.task_id,
        [InvocationStatus.SUCCESS],
        create_argument_filter(None),
    )
    result_condition = ResultCondition(
        trigger_target_task.task_id,
        create_argument_filter(None),
        create_result_filter(8),
    )
    valid_conditions = [
        ValidCondition(
            status_condition,
            StatusContext.from_invocation(older, InvocationStatus.SUCCESS),
        ),
        ValidCondition(
            status_condition,
            StatusContext.from_invocation(matching, InvocationStatus.SUCCESS),
        ),
        ValidCondition(
            result_condition,
            ResultContext(
                call_id=matching.call.call_id,
                invocation_id=matching.invocation_id,
                arguments=matching.call.arguments,
                status=InvocationStatus.SUCCESS,
                disable_cache_args=matching.call.task.conf.disable_cache_args,
                result=8,
            ),
        ),
    ]
    context = TriggerContext(
        valid_conditions={vc.valid_condition_id: vc for vc in valid_conditions}
    )
    trigger_definition = TriggerDefinition(
        task_id=trigger_target_task.task_id,
        condition_ids=[
            status_condition.condition_id,
            result_condition.condition_id,
        ],
    )
    now = datetime.now(UTC)

    record = trigger._build_trigger_run_record(
        "composite-run",
        trigger_definition,
        context,
        Mock(invocation_id="digest-invocation"),
        now,
        now,
        _test_atomic_service_run(),
    )

    assert len(record.participants) == 2
    assert {p.context_type for p in record.participants} == {
        "StatusContext",
        "ResultContext",
    }
    assert record.source_invocation_ids == [
        str(matching.invocation_id),
        str(matching.invocation_id),
    ]
    assert str(older.invocation_id) not in record.source_invocation_ids
