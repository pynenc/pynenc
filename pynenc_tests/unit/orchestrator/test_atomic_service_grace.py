"""Tests for the membership-stabilization (grace) window in
:func:`pynenc.orchestrator.atomic_service.decide_atomic_service_claim`.
"""

from datetime import UTC, datetime
from time import time

from pynenc.orchestrator.atomic_service import (
    ActiveRunnerInfo,
    AtomicServiceDecisionReason,
    decide_atomic_service_claim,
)


def _runner(
    runner_id: str,
    creation_offset: float = 0.0,
) -> ActiveRunnerInfo:
    now = time()
    creation = datetime.fromtimestamp(now + creation_offset, tz=UTC)
    return ActiveRunnerInfo(
        runner_id=runner_id,
        creation_time=creation,
        last_heartbeat=datetime.fromtimestamp(now, tz=UTC),
        allow_to_run_atomic_service=True,
    )


def test_single_runner_in_grace_window_should_not_start() -> None:
    """A lone runner inside its grace window should skip the cycle."""
    now = time()
    runner = _runner("runner-1", creation_offset=-10.0)
    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=now,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.1,
        membership_stabilization_seconds=60.0,
        max_start_slot_fraction=0.99,
    )
    assert claim.should_try_start is False
    assert claim.reason == AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE
    assert claim.assigned_runner_id == "runner-1"


def test_single_runner_outside_grace_window_should_start() -> None:
    """Once the runner has been eligible longer than the grace, it starts."""
    now = time()
    runner = _runner("runner-1", creation_offset=-120.0)
    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=now,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.1,
        membership_stabilization_seconds=60.0,
        max_start_slot_fraction=0.99,
    )
    assert claim.should_try_start is True
    assert claim.reason == AtomicServiceDecisionReason.ASSIGNED


def test_multi_runner_assigned_runner_in_grace_holds_slot() -> None:
    """When the assigned runner is still in grace the slot is held."""
    now = time()
    older = _runner("older", creation_offset=-1000.0)
    newer = _runner("newer", creation_offset=-5.0)
    runners = [older, newer]

    assigned_runner_id = None
    for r in runners:
        c = decide_atomic_service_claim(
            runner_id=r.runner_id,
            active_runners=runners,
            current_time=now,
            service_interval_minutes=1.0,
            spread_margin_minutes=0.1,
            membership_stabilization_seconds=60.0,
            max_start_slot_fraction=0.99,
        )
        if assigned_runner_id is None:
            assigned_runner_id = c.assigned_runner_id
        assert c.assigned_runner_id == assigned_runner_id

    if assigned_runner_id == "newer":
        for r in runners:
            c = decide_atomic_service_claim(
                runner_id=r.runner_id,
                active_runners=runners,
                current_time=now,
                service_interval_minutes=1.0,
                spread_margin_minutes=0.1,
                membership_stabilization_seconds=60.0,
                max_start_slot_fraction=0.99,
            )
            assert c.should_try_start is False
            assert c.reason == AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE


def test_grace_disabled_when_stabilization_is_zero() -> None:
    """With stabilization=0 the grace check is bypassed entirely."""
    now = time()
    runner = _runner("runner-1", creation_offset=-1.0)
    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=now,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.1,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.99,
    )
    assert claim.should_try_start is True
    assert claim.reason == AtomicServiceDecisionReason.ASSIGNED


def test_grace_applies_from_creation_time() -> None:
    """Creation time is the source of truth for grace-window checks."""
    now = time()
    runner = _runner("runner-1", creation_offset=-1.0)
    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=now,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.1,
        membership_stabilization_seconds=60.0,
        max_start_slot_fraction=0.99,
    )
    assert claim.should_try_start is False
    assert claim.reason == AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE
