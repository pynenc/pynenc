"""
Backend contract tests for ``BaseTrigger.auto_purge_events``.

Validates age-based retention, capacity-based retention, the disabled flag,
and the trigger-loop tick wiring for both ``MemTrigger`` and ``SQLiteTrigger``.
"""

from __future__ import annotations

from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING

import pytest

from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.trigger.monitoring import EventRecord, TriggerRunRecord

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger import BaseTrigger


def _event(event_id: str, *, timestamp: datetime, code: str = "x") -> EventRecord:
    return EventRecord(
        event_id=event_id,
        event_code=code,
        timestamp=timestamp,
        payload={},
    )


def _run(run_id: str, *, executed_at: datetime) -> TriggerRunRecord:
    return TriggerRunRecord(
        trigger_run_id=run_id,
        trigger_id="t",
        task_id_key="tasks.x",
        logic_value="and",
        executed_at=executed_at,
    )


def _test_atomic_service_run() -> AtomicServiceRun:
    return AtomicServiceRun(
        runner_id="test-runner",
        atomic_service_run_id="test-service-run",
    )


@pytest.fixture
def trigger(app_instance: Pynenc) -> BaseTrigger:
    return app_instance.trigger


def test_auto_purge_disabled_short_circuits(trigger: BaseTrigger) -> None:
    trigger.conf.event_auto_purge_enabled = False
    old = datetime.now(UTC) - timedelta(days=30)
    trigger.store_event(_event("e1", timestamp=old))

    deleted = trigger.auto_purge_events()

    assert deleted == 0
    assert trigger.get_event("e1") is not None


def test_auto_purge_age_based_event_retention(trigger: BaseTrigger) -> None:
    trigger.conf.event_retention_days = 1
    now = datetime.now(UTC)
    trigger.store_event(_event("old", timestamp=now - timedelta(days=5)))
    trigger.store_event(_event("recent", timestamp=now))

    deleted = trigger.auto_purge_events()

    assert deleted >= 1
    assert trigger.get_event("old") is None
    assert trigger.get_event("recent") is not None


def test_auto_purge_capacity_drops_oldest(trigger: BaseTrigger) -> None:
    trigger.conf.event_max_records = 2
    base = datetime.now(UTC) - timedelta(minutes=10)
    for i in range(4):
        trigger.store_event(_event(f"e{i}", timestamp=base + timedelta(minutes=i)))

    trigger.auto_purge_events()
    remaining = {e.event_id for e in trigger.get_events(limit=10)}

    assert remaining == {"e2", "e3"}


def test_auto_purge_trigger_run_retention_uses_event_days(
    trigger: BaseTrigger,
) -> None:
    """Trigger-run age retention follows ``event_retention_days``."""
    trigger.conf.event_retention_days = 1
    now = datetime.now(UTC)
    trigger.store_trigger_run(_run("old", executed_at=now - timedelta(days=5)))
    trigger.store_trigger_run(_run("recent", executed_at=now))

    trigger.auto_purge_events()

    assert trigger.get_trigger_run("old") is None
    assert trigger.get_trigger_run("recent") is not None


def test_auto_purge_trigger_run_capacity(trigger: BaseTrigger) -> None:
    trigger.conf.trigger_run_max_records = 1
    base = datetime.now(UTC) - timedelta(minutes=10)
    trigger.store_trigger_run(_run("r1", executed_at=base))
    trigger.store_trigger_run(_run("r2", executed_at=base + timedelta(minutes=1)))

    trigger.auto_purge_events()

    assert trigger.get_trigger_run("r1") is None
    assert trigger.get_trigger_run("r2") is not None


def test_trigger_loop_tick_invokes_auto_purge(trigger: BaseTrigger) -> None:
    """Every loop iteration runs the purge sweep."""
    trigger.conf.event_retention_days = 1
    old = datetime.now(UTC) - timedelta(days=5)
    trigger.store_event(_event("e1", timestamp=old))

    trigger.trigger_loop_iteration(_test_atomic_service_run())

    assert trigger.get_event("e1") is None


def test_age_purge_cascades_to_trigger_runs(trigger: BaseTrigger) -> None:
    """Trigger runs that referenced a purged event must be removed too."""
    trigger.conf.event_retention_days = 1
    old = datetime.now(UTC) - timedelta(days=5)
    fresh = datetime.now(UTC)
    trigger.store_event(_event("e_old", timestamp=old))
    trigger.store_event(_event("e_new", timestamp=fresh))
    run_referencing_old = TriggerRunRecord(
        trigger_run_id="r_old",
        trigger_id="t",
        task_id_key="tasks.x",
        logic_value="and",
        executed_at=fresh,
        event_ids=["e_old"],
    )
    run_referencing_new = TriggerRunRecord(
        trigger_run_id="r_new",
        trigger_id="t",
        task_id_key="tasks.x",
        logic_value="and",
        executed_at=fresh,
        event_ids=["e_new"],
    )
    trigger.store_trigger_run(run_referencing_old)
    trigger.store_trigger_run(run_referencing_new)

    trigger.auto_purge_events()

    assert trigger.get_event("e_old") is None
    assert trigger.get_event("e_new") is not None
    assert trigger.get_trigger_run("r_old") is None
    assert trigger.get_trigger_run("r_new") is not None
    assert trigger.get_trigger_runs_for_event("e_old") == []


def test_capacity_purge_cascades_to_trigger_runs(trigger: BaseTrigger) -> None:
    """Capacity-based purge of events drops referencing trigger runs."""
    trigger.conf.event_max_records = 1
    base = datetime.now(UTC)
    trigger.store_event(_event("e1", timestamp=base - timedelta(minutes=2)))
    trigger.store_event(_event("e2", timestamp=base))
    trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="r1",
            trigger_id="t",
            task_id_key="tasks.x",
            logic_value="and",
            executed_at=base,
            event_ids=["e1"],
        )
    )

    trigger.auto_purge_events()

    assert trigger.get_event("e1") is None
    assert trigger.get_event("e2") is not None
    assert trigger.get_trigger_run("r1") is None
