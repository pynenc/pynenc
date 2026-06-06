"""Tests for atomic-service claim construction helpers."""

from datetime import UTC, datetime

from pynenc.orchestrator.atomic_service import (
    ActiveRunnerInfo,
    AtomicServiceDecisionReason,
    decide_atomic_service_claim,
)


def create_active_runner(
    runner_id: str,
    *,
    creation_seconds: float = 0.0,
) -> ActiveRunnerInfo:
    """Create active runner metadata for claim-helper tests."""
    creation_time = datetime.fromtimestamp(creation_seconds, tz=UTC)
    return ActiveRunnerInfo(
        runner_id=runner_id,
        creation_time=creation_time,
        last_heartbeat=creation_time,
        allow_to_run_atomic_service=True,
    )


def test_decide_atomic_service_claim_should_return_run_when_assigned() -> None:
    """A runnable assigned slot returns an AtomicServiceRun and no event."""
    runner = create_active_runner("runner-1")

    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=0.0,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.5,
    )

    assert claim.atomic_service_run is not None
    assert claim.atomic_service_run.atomic_service_id.runner_id == runner.runner_id
    assert claim.skip_reason is None
    assert claim.late_start_fraction is None


def test_decide_atomic_service_claim_should_record_late_start_event_when_slot_elapsed() -> (
    None
):
    """A late assigned slot becomes a durable late-start event, not a run."""
    runner = create_active_runner("runner-1")

    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=45.0,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=0.5,
    )

    assert claim.atomic_service_run is None
    assert claim.late_start_fraction == 0.75
    assert claim.skip_reason == AtomicServiceDecisionReason.LATE_START
    assert claim.decision.runner_id == runner.runner_id
    assert "75.00%" in claim.skip_message


def test_decide_atomic_service_claim_should_record_grace_event_when_runner_stabilizing() -> (
    None
):
    """Membership grace holds the assigned slot without returning a run."""
    runner = create_active_runner("runner-1", creation_seconds=0.0)

    claim = decide_atomic_service_claim(
        runner_id=runner.runner_id,
        active_runners=[runner],
        current_time=30.0,
        service_interval_minutes=1.0,
        spread_margin_minutes=0.0,
        membership_stabilization_seconds=60.0,
        max_start_slot_fraction=1.0,
    )

    assert claim.atomic_service_run is None
    assert claim.skip_reason == AtomicServiceDecisionReason.SCHEDULED_RUNNER_IN_GRACE
    assert claim.decision.assigned_runner_id == runner.runner_id


def test_decide_atomic_service_claim_should_record_invalid_slot_event_when_margin_consumes_slot() -> (
    None
):
    """Invalid slot math becomes an event before any run is created."""
    runners = [
        create_active_runner("runner-1", creation_seconds=0.0),
        create_active_runner("runner-2", creation_seconds=1.0),
        create_active_runner("runner-3", creation_seconds=2.0),
    ]

    claim = decide_atomic_service_claim(
        runner_id="runner-1",
        active_runners=runners,
        current_time=0.0,
        service_interval_minutes=3.0,
        spread_margin_minutes=2.0,
        membership_stabilization_seconds=0.0,
        max_start_slot_fraction=1.0,
    )

    assert claim.atomic_service_run is None
    assert claim.skip_reason == AtomicServiceDecisionReason.SLOT_WINDOW_INVALID
    assert claim.decision.stable_runner_count == 3
    assert "spread_margin=2.0m" in claim.skip_message
