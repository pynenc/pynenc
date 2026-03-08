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
from unittest.mock import patch

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
    )


def test_register_runner_heartbeat(app_instance: "Pynenc") -> None:
    """Test that runner heartbeat registration works."""
    runner_ctx = create_runner_context("test-runner-1")

    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 1
    assert active_runners[0].runner_id == "test-runner-1"


def test_multiple_runner_heartbeats_ordered_by_creation(app_instance: "Pynenc") -> None:
    """Test that multiple runners are ordered by creation time."""
    runner1 = create_runner_context("runner-1")
    runner2 = create_runner_context("runner-2")
    runner3 = create_runner_context("runner-3")

    app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeats([runner3.runner_id])

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 3
    assert active_runners[0].runner_id == "runner-1"
    assert active_runners[1].runner_id == "runner-2"
    assert active_runners[2].runner_id == "runner-3"


def test_heartbeat_update_does_not_change_order(app_instance: "Pynenc") -> None:
    """Test that updating heartbeat doesn't change runner order."""
    runner1 = create_runner_context("runner-1")
    runner2 = create_runner_context("runner-2")

    app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeats([runner2.runner_id])
    sleep(0.01)
    app_instance.orchestrator.register_runner_heartbeats([runner1.runner_id])

    active_runners = app_instance.orchestrator.get_active_runners()
    assert len(active_runners) == 2
    assert active_runners[0].runner_id == "runner-1"
    assert active_runners[1].runner_id == "runner-2"


def test_get_pending_invocations_for_recovery(app_instance: "Pynenc") -> None:
    """Test retrieval of stuck pending invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 0.3

    try:
        inv1: DistributedInvocation = dummy_task()  # type: ignore
        inv2: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator._register_new_invocations([inv1, inv2])

        # Transition inv1 to PENDING
        app_instance.orchestrator._atomic_status_transition(
            inv1.invocation_id, InvocationStatus.PENDING, "owner-1"
        )

        # Wait longer than max_pending_seconds to ensure inv1 is stuck
        sleep(0.3)

        # Transition inv2 to PENDING (should not be stuck yet)
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


def test_should_run_atomic_service_single_runner(app_instance: "Pynenc") -> None:
    """Test atomic service scheduling with single runner."""
    runner_ctx = create_runner_context("runner-1")

    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    should_run = app_instance.orchestrator.should_run_atomic_service(runner_ctx)

    assert should_run is True


def test_should_run_atomic_service_multiple_runners(app_instance: "Pynenc") -> None:
    """Test atomic service scheduling distributes across runners."""
    original_interval = app_instance.conf.atomic_service_interval_minutes
    original_margin = app_instance.conf.atomic_service_spread_margin_minutes
    # Use larger interval and smaller margin so each runner gets a distinct time slot
    # With 3 runners, 60s interval, ~1s margin: each gets ~19s slot
    app_instance.conf.atomic_service_interval_minutes = 1.0
    app_instance.conf.atomic_service_spread_margin_minutes = 0.016  # ~1 second

    try:
        runner1 = create_runner_context("runner-1")
        runner2 = create_runner_context("runner-2")
        runner3 = create_runner_context("runner-3")

        # Pre-register all runners with can_run_atomic_service=True
        # This ensures all 3 are visible when checking time slots
        app_instance.orchestrator.register_runner_heartbeats(
            [runner1.runner_id], can_run_atomic_service=True
        )
        app_instance.orchestrator.register_runner_heartbeats(
            [runner2.runner_id], can_run_atomic_service=True
        )
        app_instance.orchestrator.register_runner_heartbeats(
            [runner3.runner_id], can_run_atomic_service=True
        )

        # Patch time() to a fixed value so the test is not clock-sensitive.
        # With 3 runners and a 60s interval, slots are [0,19s), [20,39s), [40,59s).
        # Setting time=0 (i.e. 0 % 60 == 0) lands inside runner1's slot so exactly
        # one runner returns True regardless of when the test runs.
        with patch("pynenc.orchestrator.base_orchestrator.time", return_value=0.0):
            results = [
                app_instance.orchestrator.should_run_atomic_service(runner1),
                app_instance.orchestrator.should_run_atomic_service(runner2),
                app_instance.orchestrator.should_run_atomic_service(runner3),
            ]

        # Only one runner should be scheduled at any given time
        assert sum(results) == 1
    finally:
        app_instance.conf.atomic_service_interval_minutes = original_interval
        app_instance.conf.atomic_service_spread_margin_minutes = original_margin


def test_invocation_recovery_service_recovers_stuck_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service reroutes stuck invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 0.1

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        sleep(0.15)  # Wait for timeout
        assert (
            inv.invocation_id
            in app_instance.orchestrator.get_pending_invocations_for_recovery()
        )
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


def test_recovery_service_ignores_recent_pending_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service ignores invocations that haven't timed out yet."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 1.0

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        # Don't wait - invoke recovery immediately
        assert not list(
            app_instance.orchestrator.get_pending_invocations_for_recovery()
        )
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


# ============================================================================
# RUNNING Invocation Recovery Tests
# ============================================================================


def test_get_running_invocations_for_recovery_from_dead_runner(
    app_instance: "Pynenc",
) -> None:
    """Test retrieval of RUNNING invocations owned by inactive runners."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001  # ~0.06s

    try:
        # Create two runners
        runner_alive = create_runner_context("runner-alive")
        runner_dead = create_runner_context("runner-dead")

        # Register both runners
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        app_instance.orchestrator.register_runner_heartbeats([runner_dead.runner_id])

        # Create invocations
        inv_alive: DistributedInvocation = dummy_task()  # type: ignore
        inv_dead: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator.register_new_invocations([inv_alive, inv_dead])

        # Set both to PENDING then RUNNING with their respective owners
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.PENDING, runner_alive
        )
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.RUNNING, runner_alive
        )
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.PENDING, runner_dead
        )
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.RUNNING, runner_dead
        )

        # Wait for runner_dead to become inactive
        sleep(0.2)

        # Keep runner_alive alive
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout


def test_running_invocation_recovery_service_recovers_dead_runner_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service reroutes RUNNING invocations from dead runners."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001

    try:
        runner_alive = create_runner_context("runner-alive")
        runner_dead = create_runner_context("runner-dead")

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        app_instance.orchestrator.register_runner_heartbeats([runner_dead.runner_id])

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])

        # Set to RUNNING owned by runner_dead
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_dead
        )
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.RUNNING, runner_dead
        )

        # Wait for runner_dead to become inactive
        sleep(0.1)

        # Invocation should be listed for recovery
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        assert (
            inv.invocation_id
            in app_instance.orchestrator.get_running_invocations_for_recovery()
        )

    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout


def test_running_recovery_ignores_invocations_from_active_runners(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service doesn't affect RUNNING invocations from active runners."""
    runner_ctx = create_runner_context("active-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

    inv: DistributedInvocation = dummy_task()  # type: ignore
    app_instance.orchestrator.register_new_invocations([inv])

    # Set to RUNNING owned by active runner
    app_instance.orchestrator.set_invocation_status(
        inv.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app_instance.orchestrator.set_invocation_status(
        inv.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )

    # Run recovery - should not affect this invocation
    assert not list(app_instance.orchestrator.get_running_invocations_for_recovery())


def test_running_recovery_handles_multiple_dead_runner_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery service handles multiple RUNNING invocations from dead runners."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001

    try:
        runner_alive = create_runner_context("runner-alive")
        runner_dead = create_runner_context("runner-dead")

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        app_instance.orchestrator.register_runner_heartbeats([runner_dead.runner_id])

        inv1: DistributedInvocation = dummy_task()  # type: ignore
        inv2: DistributedInvocation = dummy_task()  # type: ignore
        inv3: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator.register_new_invocations([inv1, inv2, inv3])

        # Set all to RUNNING owned by runner_dead
        for inv in [inv1, inv2, inv3]:
            app_instance.orchestrator.set_invocation_status(
                inv.invocation_id, InvocationStatus.PENDING, runner_dead
            )
            app_instance.orchestrator.set_invocation_status(
                inv.invocation_id, InvocationStatus.RUNNING, runner_dead
            )

        # Wait for runner_dead to become inactive
        sleep(0.1)

        # Run recovery
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        assert set(
            app_instance.orchestrator.get_running_invocations_for_recovery()
        ) == {inv1.invocation_id, inv2.invocation_id, inv3.invocation_id}
    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout


def test_running_recovery_with_mixed_dead_and_alive_runners(
    app_instance: "Pynenc",
) -> None:
    """Test recovery only affects invocations from dead runners, not alive ones."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001

    try:
        runner_alive = create_runner_context("runner-alive")
        runner_dead = create_runner_context("runner-dead")

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        app_instance.orchestrator.register_runner_heartbeats([runner_dead.runner_id])

        inv_alive: DistributedInvocation = dummy_task()  # type: ignore
        inv_dead: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator.register_new_invocations([inv_alive, inv_dead])

        # Set inv_alive to RUNNING owned by runner_alive
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.PENDING, runner_alive
        )
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.RUNNING, runner_alive
        )

        # Set inv_dead to RUNNING owned by runner_dead
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.PENDING, runner_dead
        )
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.RUNNING, runner_dead
        )

        # Wait for runner_dead to become inactive
        sleep(0.1)

        # Only invocations from dead runners should be listed for recovery
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        assert [inv_dead.invocation_id] == list(
            app_instance.orchestrator.get_running_invocations_for_recovery()
        )

    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout
