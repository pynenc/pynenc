"""
Tests for runner heartbeat tracking and invocation recovery service.

Key components tested:
- Runner heartbeat registration and tracking
- Active runner retrieval and ordering
- Inactive runner cleanup
- Recovery service scheduling logic
- Pending invocation recovery
"""

from typing import TYPE_CHECKING
from time import sleep


from pynenc.invocation import InvocationStatus
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


def create_runner_context(runner_id: str) -> RunnerContext:
    """Create a test runner context."""
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
        extra_data={},
    )


def test_register_runner_heartbeat(app_instance: "Pynenc") -> None:
    """Test that runner heartbeat registration works."""
    runner_ctx = create_runner_context("test-runner-1")

    app_instance.orchestrator.register_runner_heartbeat(runner_ctx)

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 1
    assert active_runners[0].runner_ctx.runner_id == "test-runner-1"


def test_multiple_runner_heartbeats_ordered_by_creation(app_instance: "Pynenc") -> None:
    """Test that multiple runners are ordered by creation time."""
    runner1 = create_runner_context("runner-1")
    runner2 = create_runner_context("runner-2")
    runner3 = create_runner_context("runner-3")

    app_instance.orchestrator.register_runner_heartbeat(runner1)
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeat(runner2)
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeat(runner3)

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 3
    assert active_runners[0].runner_ctx.runner_id == "runner-1"
    assert active_runners[1].runner_ctx.runner_id == "runner-2"
    assert active_runners[2].runner_ctx.runner_id == "runner-3"


def test_heartbeat_update_does_not_change_order(app_instance: "Pynenc") -> None:
    """Test that updating heartbeat doesn't change runner order."""
    runner1 = create_runner_context("runner-1")
    runner2 = create_runner_context("runner-2")

    app_instance.orchestrator.register_runner_heartbeat(runner1)
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeat(runner2)
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeat(runner1)

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 2
    assert active_runners[0].runner_ctx.runner_id == "runner-1"
    assert active_runners[1].runner_ctx.runner_id == "runner-2"


def test_cleanup_inactive_runners(app_instance: "Pynenc") -> None:
    """Test that inactive runners are cleaned up after timeout."""
    # Override timeout for testing
    original_timeout = app_instance.orchestrator.conf.runner_heartbeat_timeout_minutes
    app_instance.orchestrator.conf.runner_heartbeat_timeout_minutes = (
        0.001  # ~0.06 seconds
    )

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")

        app_instance.orchestrator.register_runner_heartbeat(runner1)
        sleep(0.1)
        app_instance.orchestrator.register_runner_heartbeat(runner2)

        app_instance.orchestrator.cleanup_inactive_runners()

        active_runners = app_instance.orchestrator.get_active_runners()
        assert len(active_runners) == 1
        assert active_runners[0].runner_ctx.runner_id == "runner-2"
    finally:
        app_instance.orchestrator.conf.runner_heartbeat_timeout_minutes = (
            original_timeout
        )


def test_get_pending_invocations_for_recovery(app_instance: "Pynenc") -> None:
    """Test retrieval of stuck pending invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 0.06

    try:
        inv1: DistributedInvocation = dummy_task()  # type: ignore
        inv2: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator._register_new_invocations([inv1, inv2])

        app_instance.orchestrator._atomic_status_transition(
            inv1.invocation_id, InvocationStatus.PENDING, "owner-1"
        )
        sleep(0.1)
        app_instance.orchestrator._atomic_status_transition(
            inv2.invocation_id, InvocationStatus.PENDING, "owner-2"
        )

        stuck_invocations = list(
            app_instance.orchestrator.get_pending_invocations_for_recovery()
        )

        assert len(stuck_invocations) == 1
        assert stuck_invocations[0] == inv1.invocation_id
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


def test_should_run_recovery_service_single_runner(app_instance: "Pynenc") -> None:
    """Test recovery service scheduling with single runner."""
    runner_ctx = create_runner_context("runner-1")

    app_instance.orchestrator.register_runner_heartbeat(runner_ctx)

    should_run = app_instance.orchestrator.should_run_recovery_service(runner_ctx)

    assert should_run is True


def test_should_run_recovery_service_multiple_runners(app_instance: "Pynenc") -> None:
    """Test recovery service scheduling distributes across runners."""
    original_interval = (
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes
    )
    app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = 1.0

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")
        runner3 = create_runner_context("runner-3")

        app_instance.orchestrator.register_runner_heartbeat(runner1)
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeat(runner2)
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeat(runner3)

        results = [
            app_instance.orchestrator.should_run_recovery_service(runner1),
            app_instance.orchestrator.should_run_recovery_service(runner2),
            app_instance.orchestrator.should_run_recovery_service(runner3),
        ]

        # Only one runner should be scheduled at any given time
        assert sum(results) <= 1
    finally:
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = (
            original_interval
        )


def test_invocation_recovery_service_recovers_stuck_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service reroutes stuck invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    original_interval = (
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes
    )
    app_instance.conf.max_pending_seconds = 0.06
    app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = 0.01

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeat(runner_ctx)

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        sleep(0.1)
        app_instance.orchestrator.invocation_recovery_service(runner_ctx)

        status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        assert status == InvocationStatus.REROUTED
    finally:
        app_instance.conf.max_pending_seconds = original_timeout
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = (
            original_interval
        )


def test_recovery_service_skips_when_not_scheduled(app_instance: "Pynenc") -> None:
    """Test that recovery service is skipped when runner is not scheduled."""
    original_timeout = app_instance.conf.max_pending_seconds
    original_interval = (
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes
    )
    app_instance.conf.max_pending_seconds = 0.06
    app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = 100.0

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")

        app_instance.orchestrator.register_runner_heartbeat(runner1)
        sleep(0.01)
        app_instance.orchestrator.register_runner_heartbeat(runner2)

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator._register_new_invocations([inv])
        app_instance.orchestrator._atomic_status_transition(
            inv.invocation_id, InvocationStatus.PENDING, "owner-1"
        )

        sleep(0.1)

        # Runner 2 is not scheduled to run recovery at this time
        app_instance.orchestrator.invocation_recovery_service(runner2)

        status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        assert status == InvocationStatus.PENDING
    finally:
        app_instance.conf.max_pending_seconds = original_timeout
        app_instance.orchestrator.conf.run_invocation_recovery_service_every_minutes = (
            original_interval
        )


def test_recovery_service_updates_heartbeat(app_instance: "Pynenc") -> None:
    """Test that recovery service always updates heartbeat."""
    runner_ctx = create_runner_context("runner-1")

    app_instance.orchestrator.register_runner_heartbeat(runner_ctx)
    sleep(0.05)

    app_instance.orchestrator.invocation_recovery_service(runner_ctx)

    second_runners = app_instance.orchestrator.get_active_runners()
    assert len(second_runners) == 1
    assert second_runners[0].runner_ctx.runner_id == runner_ctx.runner_id
