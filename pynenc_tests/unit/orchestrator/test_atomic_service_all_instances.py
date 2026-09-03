"""
Tests for atomic service coordination logic.

Key components tested:
- Runner position calculation in ordered list
- Time slot calculation for distributed execution
- Time slot membership checking
- Overall service execution decision logic
- Execution time validation
"""

from datetime import UTC, datetime, timedelta
from time import sleep, time
from typing import TYPE_CHECKING

import pytest

from pynenc.orchestrator import atomic_service
from pynenc.orchestrator.atomic_service import AtomicServiceRun
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from pynenc import Pynenc


def create_runner_context(runner_id: str) -> RunnerContext:
    """Create a test runner context."""
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
    )


def create_atomic_service_run(
    runner_id: str,
    atomic_service_run_id: str,
    started_at: datetime | None = None,
) -> AtomicServiceRun:
    """Create a test atomic-service run identity."""
    return AtomicServiceRun(
        runner_id=runner_id,
        atomic_service_run_id=atomic_service_run_id,
        started_at=started_at,
    )


def record_atomic_service_execution(
    app_instance: "Pynenc",
    runner_ctx: RunnerContext,
    atomic_service_run_id: str,
    start_time: datetime,
    end_time: datetime,
) -> None:
    """Record an atomic-service execution without going through scheduling."""
    atomic_service_run = create_atomic_service_run(
        runner_ctx.runner_id,
        atomic_service_run_id,
        started_at=start_time,
    )
    app_instance.orchestrator.record_atomic_service_execution_start(
        atomic_service_run, start_time
    )
    app_instance.orchestrator.finalize_atomic_service_execution(
        atomic_service_run,
        end_time,
        atomic_service.AtomicServiceExecutionStatus.COMPLETED,
    )


def create_active_runner_info(
    runner_id: str,
    creation_offset: float = 0.0,
    last_service_duration: float | None = None,
    service_running: bool = False,
) -> atomic_service.ActiveRunnerInfo:
    """Create an ActiveRunnerInfo for testing."""
    current = time()

    del last_service_duration, service_running

    return atomic_service.ActiveRunnerInfo(
        runner_id=runner_id,
        creation_time=datetime.fromtimestamp(current + creation_offset, tz=UTC),
        last_heartbeat=datetime.fromtimestamp(current, tz=UTC),
    )


def test_decide_atomic_service_claim_should_return_true_when_single_runner() -> None:
    """A single eligible runner is always assigned its own slot."""
    current_time = datetime(2026, 1, 1, 12, 0, tzinfo=UTC)
    runner = atomic_service.ActiveRunnerInfo(
        runner_id="runner-1",
        creation_time=current_time,
        last_heartbeat=current_time,
    )
    active_runners = [runner]

    result = atomic_service.decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=active_runners,
        current_time=current_time.timestamp(),
        service_interval_minutes=5.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )

    assert result.should_try_start is True


def test_decide_atomic_service_claim_should_return_false_when_no_runners() -> None:
    """An empty runner set yields no stable runners and no claim."""
    runner = create_runner_context("runner-1")
    active_runners: list[atomic_service.ActiveRunnerInfo] = []

    result = atomic_service.decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=active_runners,
        current_time=time(),
        service_interval_minutes=5.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )

    assert result.should_try_start is False
    assert result.reason == atomic_service.AtomicServiceDecisionReason.NO_STABLE_RUNNERS


def test_decide_atomic_service_claim_should_distribute_execution_when_multiple_runners() -> (
    None
):
    """Test that multiple runners get non-overlapping time slots."""
    runner1 = create_active_runner_info("runner-1", -2.0)
    runner2 = create_active_runner_info("runner-2", -1.0)
    runner3 = create_active_runner_info("runner-3", 0.0)
    active_runners = [runner1, runner2, runner3]

    # 6 minute interval = 360 seconds, 3 runners, 1 minute margin
    # Slots: [0,60), [120,180), [240,300)

    result1 = atomic_service.decide_atomic_service_claim(
        runner_id=runner1.runner_id,
        active_runners=active_runners,
        current_time=0.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )
    result2 = atomic_service.decide_atomic_service_claim(
        runner_id=runner2.runner_id,
        active_runners=active_runners,
        current_time=150.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )
    result3 = atomic_service.decide_atomic_service_claim(
        runner_id=runner3.runner_id,
        active_runners=active_runners,
        current_time=250.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )
    result1_wrong_time = atomic_service.decide_atomic_service_claim(
        runner_id=runner1.runner_id,
        active_runners=active_runners,
        current_time=150.0,
        service_interval_minutes=6.0,
        spread_margin_minutes=1.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )

    assert result1.should_try_start is True
    assert result2.should_try_start is True
    assert result3.should_try_start is True
    assert result1_wrong_time.should_try_start is False
    assert (
        result1_wrong_time.reason
        == atomic_service.AtomicServiceDecisionReason.NOT_ASSIGNED_SLOT
    )


def test_sqlite_orchestrator_can_run_atomic_service_filter(
    app_instance: "Pynenc",
) -> None:
    """Test that register_runner_heartbeat and get_active_runners filter by can_run_atomic_service."""
    orchestrator = app_instance.orchestrator

    ctx_true = create_runner_context("runner-true")
    ctx_false = create_runner_context("runner-false")
    orchestrator.register_runner_heartbeats(
        [ctx_true.runner_id], can_run_atomic_service=True
    )
    orchestrator.register_runner_heartbeats(
        [ctx_false.runner_id], can_run_atomic_service=False
    )

    # Only runner with can_run_atomic_service=True
    runners_true = orchestrator.get_active_runners(can_run_atomic_service=True)
    assert len(runners_true) == 1
    assert runners_true[0].runner_id == ctx_true.runner_id

    # Only runner with can_run_atomic_service=False
    runners_false = orchestrator.get_active_runners(can_run_atomic_service=False)
    assert len(runners_false) == 1
    assert runners_false[0].runner_id == ctx_false.runner_id

    # All runners
    runners_all = orchestrator.get_active_runners(can_run_atomic_service=None)
    ids_all = {r.runner_id for r in runners_all}
    assert {ctx_true.runner_id, ctx_false.runner_id} == ids_all


def test_should_run_atomic_service_should_respect_time_slots_with_multiple_runners(
    app_instance: "Pynenc",
) -> None:
    """Test that atomic service scheduling assigns different time slots to runners."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    app_instance.conf.atomic_service_interval_minutes = 100.0

    try:
        # Mock time to be at the start of the cycle
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            runner1 = create_runner_context("runner-1")
            runner2 = create_runner_context("runner-2")

            app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
            sleep(0.01)
            app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])

            # At time=0, runner1 should be scheduled, runner2 should not
            claim_1 = app_instance.orchestrator.try_claim_atomic_service_run(runner1)
            claim_2 = app_instance.orchestrator.try_claim_atomic_service_run(runner2)

            assert claim_1 is not None, "Runner 1 should be able to claim at time=0"
            assert claim_2 is None, "Runner 2 should NOT be able to claim at time=0"
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval


def test_should_run_atomic_service_should_handle_runner_cycling(
    app_instance: "Pynenc",
) -> None:
    """Test that runners get scheduled in rotation over time."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")
        runner3 = create_runner_context("runner-3")

        app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats([runner3.runner_id])

        # Test at different points in the cycle
        # 60min interval / 3 runners = 20min slots each
        # Runner1: [0-19min), Runner2: [20-39min), Runner3: [40-59min)

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0):
            claim1 = app_instance.orchestrator.try_claim_atomic_service_run(runner1)
            assert claim1 is not None
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner2) is None
            )
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner3) is None
            )
            # Record completion of runner1's execution before moving to next time slot
            start_time = datetime.fromtimestamp(0, tz=UTC)
            end_time = start_time + timedelta(seconds=10)
            app_instance.orchestrator.finalize_atomic_service_execution(
                claim1,
                end_time,
                atomic_service.AtomicServiceExecutionStatus.COMPLETED,
            )

        with patch(
            "pynenc.orchestrator.base_orchestrator.time", return_value=1200
        ):  # 20min
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner1) is None
            )
            claim2 = app_instance.orchestrator.try_claim_atomic_service_run(runner2)
            assert claim2 is not None
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner3) is None
            )
            # Record completion of runner2's execution
            start_time = datetime.fromtimestamp(1200, tz=UTC)
            end_time = start_time + timedelta(seconds=10)
            app_instance.orchestrator.finalize_atomic_service_execution(
                claim2,
                end_time,
                atomic_service.AtomicServiceExecutionStatus.COMPLETED,
            )

        with patch(
            "pynenc.orchestrator.base_orchestrator.time", return_value=2400
        ):  # 40min
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner1) is None
            )
            assert (
                app_instance.orchestrator.try_claim_atomic_service_run(runner2) is None
            )
            claim3 = app_instance.orchestrator.try_claim_atomic_service_run(runner3)
            assert claim3 is not None
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval


def test_record_atomic_service_execution_should_update_timestamps(
    app_instance: "Pynenc",
) -> None:
    """Test that atomic service execution timestamps are recorded."""
    runner_ctx = create_runner_context("test-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    start_time = datetime.now(UTC)
    sleep(0.01)
    end_time = datetime.now(UTC)

    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-1", start_time, end_time
    )

    executions = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        datetime.fromtimestamp(0, tz=UTC),
        datetime.now(UTC),
        limit=10,
        runner_id=runner_ctx.runner_id,
    )
    assert len(executions) == 1
    execution = executions[0]
    assert abs((execution.start_time - start_time).total_seconds()) < 0.001
    assert execution.end_time is not None
    assert abs((execution.end_time - end_time).total_seconds()) < 0.001


def test_record_atomic_service_execution_should_keep_history(
    app_instance: "Pynenc",
) -> None:
    """Atomic-service monitoring keeps prior executions, not only the latest one."""
    runner_ctx = create_runner_context("test-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    first_start = datetime.now(UTC) - timedelta(seconds=30)
    first_end = first_start + timedelta(milliseconds=2)
    second_start = first_start + timedelta(seconds=10)
    second_end = second_start + timedelta(milliseconds=3)

    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-1", first_start, first_end
    )
    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-2", second_start, second_end
    )

    executions = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        first_start - timedelta(seconds=1),
        second_end + timedelta(seconds=1),
    )

    assert [execution.start_time for execution in executions] == [
        second_start,
        first_start,
    ]
    assert executions[0].runner_id == runner_ctx.runner_id
    assert [execution.atomic_service_run_id for execution in executions] == [
        "service-run-2",
        "service-run-1",
    ]


def test_atomic_service_executions_age_purge(app_instance: "Pynenc") -> None:
    """Records older than ``atomic_service_execution_retention_minutes`` are dropped."""
    app_instance.conf.atomic_service_execution_retention_minutes = 1.0
    app_instance.conf.atomic_service_execution_max_records = 1000

    runner_ctx = create_runner_context("aged-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    now = datetime.now(UTC)
    old_start = now - timedelta(minutes=5)
    old_end = old_start + timedelta(milliseconds=2)
    fresh_start = now - timedelta(seconds=10)
    fresh_end = fresh_start + timedelta(milliseconds=2)

    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-old", old_start, old_end
    )
    # The second record triggers another purge that should remove the old one.
    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-fresh", fresh_start, fresh_end
    )

    executions = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        old_start - timedelta(minutes=1),
        fresh_end + timedelta(minutes=1),
    )
    assert [execution.start_time for execution in executions] == [fresh_start]


def test_atomic_service_executions_capacity_purge(app_instance: "Pynenc") -> None:
    """Only the newest ``atomic_service_execution_max_records`` records are kept."""
    app_instance.conf.atomic_service_execution_retention_minutes = 60.0
    app_instance.conf.atomic_service_execution_max_records = 3

    runner_ctx = create_runner_context("capacity-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    base = datetime.now(UTC) - timedelta(seconds=30)
    for offset in range(5):
        start = base + timedelta(seconds=offset)
        record_atomic_service_execution(
            app_instance,
            runner_ctx,
            f"service-run-{offset}",
            start,
            start + timedelta(milliseconds=2),
        )

    executions = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        base - timedelta(seconds=1),
        base + timedelta(minutes=1),
    )
    assert len(executions) == 3
    # Newest first; the oldest two should have been trimmed.
    assert [execution.start_time for execution in executions] == [
        base + timedelta(seconds=4),
        base + timedelta(seconds=3),
        base + timedelta(seconds=2),
    ]


def test_atomic_service_executions_age_purge_protects_referenced_run(
    app_instance: "Pynenc",
) -> None:
    """Executions referenced by trigger-run history survive age-based purge.

    The orchestrator asks the trigger backend for the set of referenced
    ``atomic_service_run_id`` values and skips those rows. The protection
    is the only cross-store guarantee in the codebase; verify it on every
    parametrized backend so a Pynmon trigger-run page never resolves to a
    purged execution.
    """
    from pynenc.trigger.monitoring import TriggerRunRecord

    # Keep auto-purge disabled during recording; we trigger the explicit
    # purge below so the trigger-run reference is in place by then.
    app_instance.conf.atomic_service_execution_retention_minutes = 0.0
    app_instance.conf.atomic_service_execution_max_records = 0

    runner_ctx = create_runner_context("protected-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    now = datetime.now(UTC)
    old_start = now - timedelta(minutes=10)
    old_end = old_start + timedelta(milliseconds=2)
    record_atomic_service_execution(
        app_instance, runner_ctx, "service-run-protected", old_start, old_end
    )

    app_instance.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-1",
            trigger_id="trig-1",
            task_id_key="tasks.x",
            logic_value="and",
            atomic_service_run_id="service-run-protected",
            atomic_service_runner_id=runner_ctx.runner_id,
            claimed_at=now,
            executed_at=now,
        )
    )

    referenced = app_instance.trigger.get_referenced_atomic_service_run_ids()
    assert "service-run-protected" in referenced

    app_instance.conf.atomic_service_execution_retention_minutes = 1.0
    app_instance.conf.atomic_service_execution_max_records = 1000
    app_instance.orchestrator.purge_atomic_service_executions()

    survivors = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        old_start - timedelta(minutes=1),
        now + timedelta(minutes=1),
    )
    assert [execution.atomic_service_run_id for execution in survivors] == [
        "service-run-protected"
    ]


def test_atomic_service_executions_capacity_purge_protects_referenced_run(
    app_instance: "Pynenc",
) -> None:
    """Capacity-based purge also honours trigger-run references."""
    from pynenc.trigger.monitoring import TriggerRunRecord

    # Keep auto-purge disabled during recording; the cap is enforced via the
    # explicit purge below after the trigger-run reference is in place.
    app_instance.conf.atomic_service_execution_retention_minutes = 0.0
    app_instance.conf.atomic_service_execution_max_records = 0

    runner_ctx = create_runner_context("protected-cap-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    base = datetime.now(UTC) - timedelta(seconds=30)
    for offset in range(5):
        start = base + timedelta(seconds=offset)
        record_atomic_service_execution(
            app_instance,
            runner_ctx,
            f"service-run-{offset}",
            start,
            start + timedelta(milliseconds=2),
        )

    # The oldest record (offset=0) would normally be trimmed by the cap.
    app_instance.trigger.store_trigger_run(
        TriggerRunRecord(
            trigger_run_id="run-cap",
            trigger_id="trig-1",
            task_id_key="tasks.x",
            logic_value="and",
            atomic_service_run_id="service-run-0",
            atomic_service_runner_id=runner_ctx.runner_id,
            claimed_at=base,
            executed_at=base,
        )
    )

    app_instance.conf.atomic_service_execution_retention_minutes = 60.0
    app_instance.conf.atomic_service_execution_max_records = 2
    app_instance.orchestrator.purge_atomic_service_executions()

    survivors = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        base - timedelta(seconds=1),
        base + timedelta(minutes=1),
    )
    survivor_ids = {execution.atomic_service_run_id for execution in survivors}
    assert "service-run-0" in survivor_ids


def test_atomic_service_executions_filter_by_runner_and_duration(
    app_instance: "Pynenc",
) -> None:
    """``runner_id`` and ``min_duration_seconds`` filter the returned records."""
    app_instance.conf.atomic_service_execution_retention_minutes = 60.0

    runner_a = create_runner_context("runner-a")
    runner_b = create_runner_context("runner-b")
    app_instance.orchestrator.register_runner_heartbeats(
        [runner_a.runner_id, runner_b.runner_id]
    )

    now = datetime.now(UTC)
    short_start = now - timedelta(seconds=20)
    short_end = short_start + timedelta(milliseconds=5)
    long_start = now - timedelta(seconds=10)
    long_end = long_start + timedelta(seconds=1, milliseconds=200)

    record_atomic_service_execution(
        app_instance, runner_a, "service-run-short", short_start, short_end
    )
    record_atomic_service_execution(
        app_instance, runner_b, "service-run-long", long_start, long_end
    )

    start_window = short_start - timedelta(seconds=1)
    end_window = long_end + timedelta(seconds=1)

    only_a = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        start_window, end_window, runner_id=runner_a.runner_id
    )
    assert [execution.runner_id for execution in only_a] == [runner_a.runner_id]

    long_only = app_instance.orchestrator.get_atomic_service_executions_in_timerange(
        start_window, end_window, min_duration_seconds=1.0
    )
    assert [execution.runner_id for execution in long_only] == [runner_b.runner_id]


# --- Tests for orchestrator.try_claim_atomic_service_run (Slice L) ---


def test_try_claim_atomic_service_run_returns_id_when_eligible(
    app_instance: "Pynenc",
) -> None:
    """A single eligible runner gets a non-None AtomicServiceRun."""
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    try:
        runner_ctx = create_runner_context("solo-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        result = app_instance.orchestrator.try_claim_atomic_service_run(runner_ctx)
        assert isinstance(result, AtomicServiceRun)
        assert isinstance(result.atomic_service_run_id, str)
        assert len(result.atomic_service_run_id) > 0
        assert result.runner_id == runner_ctx.runner_id
        assert result.started_at is not None
    finally:
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction


def test_try_claim_atomic_service_run_returns_none_when_not_in_time_slot(
    app_instance: "Pynenc",
) -> None:
    """Runner not in its time slot does not claim."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    app_instance.conf.atomic_service_interval_minutes = 100.0
    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")
        app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            r1 = app_instance.orchestrator.try_claim_atomic_service_run(runner1)
            r2 = app_instance.orchestrator.try_claim_atomic_service_run(runner2)
        assert r1 is not None
        assert r2 is None
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval


def test_try_claim_atomic_service_run_returns_none_when_slot_is_too_late(
    app_instance: "Pynenc",
) -> None:
    """Late-start abort happens before BaseRunner receives a run."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    app_instance.conf.atomic_service_interval_minutes = 1.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 0.5
    try:
        runner_ctx = create_runner_context("late-claim-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=45.0):
            result = app_instance.orchestrator.try_claim_atomic_service_run(runner_ctx)

        assert result is None
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction


def test_try_claim_atomic_service_run_late_check_disabled_when_fraction_is_one(
    app_instance: "Pynenc",
) -> None:
    """max_start_slot_fraction=1.0 disables the late-start claim check."""
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    app_instance.conf.atomic_service_interval_minutes = 1.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    try:
        runner_ctx = create_runner_context("late-disabled-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=45.0):
            result = app_instance.orchestrator.try_claim_atomic_service_run(runner_ctx)

        assert isinstance(result, AtomicServiceRun)
        assert result.runner_id == runner_ctx.runner_id
        executions = (
            app_instance.orchestrator.get_atomic_service_executions_in_timerange(
                datetime.fromtimestamp(0, tz=UTC),
                datetime.now(UTC),
                limit=10,
            )
        )
        assert not any(
            e.status == atomic_service.AtomicServiceExecutionStatus.BLOCKED
            for e in executions
        )
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction


def test_check_atomic_services_forwards_atomic_service_to_loop(
    app_instance: "Pynenc",
) -> None:
    """_check_atomic_services must forward an AtomicServiceRun to
    ``trigger_loop_iteration`` so cron-fired trigger runs record their origin."""
    from unittest.mock import patch

    runner_ctx = create_runner_context("ctx-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    captured: dict[str, object] = {}

    def fake_loop(atomic_service_run: AtomicServiceRun) -> None:
        captured["atomic_service_run"] = atomic_service_run

    with patch.object(
        app_instance.trigger, "trigger_loop_iteration", side_effect=fake_loop
    ):
        from pynenc.runner.base_runner import BaseRunner

        runner = app_instance.runner
        assert isinstance(runner, BaseRunner)
        with (
            patch.object(
                app_instance.orchestrator,
                "try_claim_atomic_service_run",
                return_value=AtomicServiceRun(
                    runner_id=runner.runner_id,
                    atomic_service_run_id="known-service-run-id",
                ),
            ),
            patch.object(
                app_instance.orchestrator, "finalize_atomic_service_execution"
            ),
        ):
            runner._check_atomic_services()  # type: ignore[attr-defined]

    atomic_service_run = captured["atomic_service_run"]
    assert isinstance(atomic_service_run, AtomicServiceRun)
    assert atomic_service_run.atomic_service_run_id == "known-service-run-id"
    assert atomic_service_run.runner_id == runner.runner_id


def test_trigger_loop_iteration_forwards_explicit_atomic_service(
    app_instance: "Pynenc",
) -> None:
    """The trigger loop consumes the exact AtomicServiceRun it is given."""
    from unittest.mock import patch

    captured: dict[str, object] = {}

    def fake_process(self: object, atomic_service_run: AtomicServiceRun) -> None:
        captured["atomic_service_run"] = atomic_service_run

    with patch.object(
        type(app_instance.trigger),
        "_process_loop_iteration",
        fake_process,
    ):
        expected = AtomicServiceRun(
            runner_id="direct-runner",
            atomic_service_run_id="direct-service-run",
        )
        app_instance.trigger.trigger_loop_iteration(expected)

    atomic_service_run = captured["atomic_service_run"]
    assert atomic_service_run is expected


def test_check_time_based_triggers_skipped_without_atomic_service(
    app_instance: "Pynenc",
) -> None:
    """Cron evaluation requires the caller to pass an AtomicServiceRun.

    This guarantees every cron tick is attributable to a real, registered
    atomic-service runner / atomic_service_run_id and cannot be silently fired
    from a worker side effect.
    """
    from datetime import datetime as _dt

    from pynenc.trigger.conditions import CronCondition

    cron = CronCondition("0 * * * *")
    app_instance.trigger.register_condition(cron)
    before = len(app_instance.trigger.get_valid_conditions())

    with pytest.raises(TypeError):
        app_instance.trigger.check_time_based_triggers(_dt(2024, 1, 1, 12, 0, 0))  # type: ignore[call-arg]

    after = len(app_instance.trigger.get_valid_conditions())
    assert after == before, (
        "check_time_based_triggers must not record any valid condition "
        "when no atomic_service_run is provided"
    )


def test_check_time_based_triggers_runs_with_atomic_service(
    app_instance: "Pynenc",
) -> None:
    """Cron evaluation runs normally when an atomic_service pair is passed."""
    from datetime import datetime as _dt
    from unittest.mock import patch

    from pynenc.trigger.conditions import CronCondition, CronContext

    cron = CronCondition("0 * * * *")
    app_instance.trigger.register_condition(cron)

    test_time = _dt(2024, 1, 1, 12, 0, 0)
    fake_ctx_value = CronContext(timestamp=test_time)

    with patch.object(CronCondition, "is_satisfied_by", return_value=True):
        with patch(
            "pynenc.trigger.conditions.CronContext", return_value=fake_ctx_value
        ):
            app_instance.trigger.check_time_based_triggers(
                test_time,
                atomic_service_run=AtomicServiceRun(
                    runner_id="real-runner",
                    atomic_service_run_id="real-service-run",
                ),
            )
    valid_ids = [
        vc.condition.condition_id
        for vc in app_instance.trigger.get_valid_conditions().values()
    ]
    assert cron.condition_id in valid_ids


def test_try_claim_atomic_service_run_ordering_stable_when_new_runner_joins(
    app_instance: "Pynenc",
) -> None:
    """A new runner joining mid-cycle never shifts existing slot ownership.

    Ordering is ``(creation_time ASC, runner_id ASC)``. A late-arriving runner
    sorts to the end, so the runner that already owned the current slot
    continues to own it (no consensus flip-flop).
    """
    from unittest.mock import patch

    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    original_fraction = app_instance.conf.atomic_service_max_start_slot_fraction
    original_retention = app_instance.conf.atomic_service_execution_retention_minutes
    app_instance.conf.atomic_service_interval_minutes = 60.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.0
    app_instance.conf.atomic_service_max_start_slot_fraction = 1.0
    app_instance.conf.atomic_service_execution_retention_minutes = 0
    try:
        runner1 = create_runner_context("stable-runner-1")
        runner2 = create_runner_context("stable-runner-2")
        app_instance.orchestrator.register_runner_heartbeats(
            [runner1.runner_id], can_run_atomic_service=True
        )
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeats(
            [runner2.runner_id], can_run_atomic_service=True
        )

        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            r1 = app_instance.orchestrator.try_claim_atomic_service_run(runner1)
            r2 = app_instance.orchestrator.try_claim_atomic_service_run(runner2)
        assert r1 is not None
        assert r2 is None
        app_instance.orchestrator.finalize_atomic_service_execution(
            r1,
            datetime.fromtimestamp(1.0, tz=UTC),
            atomic_service.AtomicServiceExecutionStatus.COMPLETED,
        )

        # A third runner joins later. It must NOT take ownership of the slot
        # that runner1 already owned, nor shift runner2's slot earlier.
        sleep(0.01)
        runner3 = create_runner_context("stable-runner-3")
        app_instance.orchestrator.register_runner_heartbeats(
            [runner3.runner_id], can_run_atomic_service=True
        )

        # At the same time-in-cycle as before, runner1 still owns the slot.
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=2.0):
            # runner1 cannot re-claim a slot it already used this cycle (BLOCKED
            # by prior_running or routine non-win), but importantly the new
            # runner3 must NOT be able to claim runner1's slot.
            r3_first_slot = app_instance.orchestrator.try_claim_atomic_service_run(
                runner3
            )
        assert r3_first_slot is None, (
            "runner3 must not steal runner1's already-owned slot when joining late"
        )
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin
        app_instance.conf.atomic_service_max_start_slot_fraction = original_fraction
        app_instance.conf.atomic_service_execution_retention_minutes = (
            original_retention
        )
