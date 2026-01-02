"""
Tests for orchestrator dead runner detection and invocation recovery.

Key components tested:
- Timeout configuration and its dual purposes (recovery + atomic service)
- False positive prevention: healthy runners never marked as dead
- Edge cases with timeout boundaries
"""

from typing import TYPE_CHECKING

from pynenc.runner.runner_context import RunnerContext
from pynenc.invocation.status import InvocationStatus
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation

# Create app and tasks for testing
app = MockPynenc()


@app.task
def dummy_task() -> None:
    pass


@app.task
def another_task(value: int) -> int:
    return value * 2


def create_runner_context(runner_id: str) -> RunnerContext:
    """Create a test runner context."""
    return RunnerContext(
        runner_cls="TestRunner",
        runner_id=runner_id,
        pid=12345,
        hostname="test-host",
        extra_data={},
    )


def create_running_invocation(
    app_instance: "Pynenc", runner_ctx: RunnerContext, task_value: int = 0
) -> str:
    """
    Helper to create a RUNNING invocation owned by the specified runner.

    :param Pynenc app_instance: The Pynenc app instance
    :param RunnerContext runner_ctx: The runner context that owns this invocation
    :param int task_value: Value to pass to the task (for uniqueness)
    :return: The invocation ID
    """
    # Assign tasks to this app instance
    dummy_task.app = app_instance
    another_task.app = app_instance

    # Create invocation
    inv: DistributedInvocation = another_task(task_value)  # type: ignore

    # Register it in the orchestrator
    app_instance.orchestrator._register_new_invocations([inv])

    # Transition to RUNNING with the specified runner
    app_instance.orchestrator._atomic_status_transition(
        inv.invocation_id, InvocationStatus.PENDING, runner_ctx.runner_id
    )
    app_instance.orchestrator._atomic_status_transition(
        inv.invocation_id, InvocationStatus.RUNNING, runner_ctx.runner_id
    )

    return inv.invocation_id


# ################################################################################### #
# DEAD RUNNER DETECTION TESTS
# ################################################################################### #


def test_get_active_runners_includes_heartbeating_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that a runner with recent heartbeat is in active list."""
    runner_ctx = create_runner_context("runner-1")

    # Register heartbeat
    app_instance.orchestrator.register_runner_heartbeat(
        runner_ctx, can_run_atomic_service=False
    )

    # Get active runners with 10-second timeout
    active_runners = app_instance.orchestrator._get_active_runners(
        timeout_seconds=10.0, can_run_atomic_service=False
    )
    runner_ids = {r.runner_ctx.runner_id for r in active_runners}

    assert runner_ctx.runner_id in runner_ids


def test_get_active_runners_excludes_stale_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that a runner without recent heartbeat is excluded."""
    runner_ctx = create_runner_context("runner-stale")

    # Register heartbeat for this runner (creates it in the system)
    app_instance.orchestrator.register_runner_heartbeat(
        runner_ctx, can_run_atomic_service=False
    )

    # Check with zero timeout - runner should immediately be stale
    active_runners = app_instance.orchestrator._get_active_runners(
        timeout_seconds=0.0, can_run_atomic_service=False
    )
    runner_ids = {r.runner_ctx.runner_id for r in active_runners}

    # With zero timeout, runner should not be active
    assert runner_ctx.runner_id not in runner_ids


def test_cleanup_removes_inactive_runners(
    app_instance: "Pynenc",
) -> None:
    """Test that cleanup_inactive_runners removes stale runner records."""
    runner_ctx = create_runner_context("runner-to-cleanup")

    # Register heartbeat
    app_instance.orchestrator.register_runner_heartbeat(
        runner_ctx, can_run_atomic_service=False
    )

    # Verify runner is active
    active_before = app_instance.orchestrator._get_active_runners(
        timeout_seconds=100.0, can_run_atomic_service=False
    )
    runner_ids_before = {r.runner_ctx.runner_id for r in active_before}
    assert runner_ctx.runner_id in runner_ids_before

    # Cleanup with zero timeout
    app_instance.orchestrator._cleanup_inactive_runners(timeout_seconds=0.0)

    # Runner should now be removed
    active_after = app_instance.orchestrator._get_active_runners(
        timeout_seconds=100.0, can_run_atomic_service=False
    )
    runner_ids_after = {r.runner_ctx.runner_id for r in active_after}

    assert runner_ctx.runner_id not in runner_ids_after


# ################################################################################### #
# INVOCATION RECOVERY TESTS
# ################################################################################### #


def test_get_running_invocations_for_recovery_finds_stuck_tasks(
    app_instance: "Pynenc",
) -> None:
    """Test that invocations owned by dead runners are found."""
    dead_runner = create_runner_context("dead-runner")

    # Create RUNNING invocation owned by dead runner (no heartbeat)
    invocation_id = create_running_invocation(app_instance, dead_runner, task_value=1)

    # With zero timeout, this should be in recovery
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    assert invocation_id in recoverable


def test_running_invocations_not_recovered_if_runner_alive(
    app_instance: "Pynenc",
) -> None:
    """Test that invocations from alive runners are NOT recovered."""
    alive_runner = create_runner_context("alive-runner")

    # Register heartbeat (runner is alive)
    app_instance.orchestrator.register_runner_heartbeat(
        alive_runner, can_run_atomic_service=False
    )

    # Create RUNNING invocation
    invocation_id = create_running_invocation(app_instance, alive_runner, task_value=2)

    # With normal timeout, should NOT be in recovery
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=600.0  # 10 minutes
        )
    )

    assert invocation_id not in recoverable


def test_multiple_invocations_recovered_from_dead_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that all invocations from a dead runner are recovered."""
    dead_runner = create_runner_context("dead-runner-multi")

    # Create multiple RUNNING invocations for the dead runner
    invocation_ids = [
        create_running_invocation(app_instance, dead_runner, task_value=i)
        for i in range(3)
    ]

    # Find all recoverable invocations
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    # All invocations should be found
    for inv_id in invocation_ids:
        assert inv_id in recoverable


def test_only_running_invocations_recovered(
    app_instance: "Pynenc",
) -> None:
    """Test that only RUNNING invocations are recovered, not other statuses."""
    dead_runner = create_runner_context("dead-runner-status-check")

    # Create RUNNING invocation
    running_inv = create_running_invocation(app_instance, dead_runner, task_value=10)

    # Create and complete FAILED invocation
    failed_inv = create_running_invocation(app_instance, dead_runner, task_value=11)
    app_instance.orchestrator._atomic_status_transition(
        failed_inv, InvocationStatus.FAILED, dead_runner.runner_id
    )

    # Create and complete SUCCESS invocation
    success_inv = create_running_invocation(app_instance, dead_runner, task_value=12)
    app_instance.orchestrator._atomic_status_transition(
        success_inv, InvocationStatus.SUCCESS, dead_runner.runner_id
    )

    # Find recoverable with zero timeout
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    # Only the RUNNING invocation should be in recovery
    assert running_inv in recoverable
    assert failed_inv not in recoverable
    assert success_inv not in recoverable


# ################################################################################### #
# TIMEOUT CONFIGURATION TESTS
# ################################################################################### #


def test_recovery_uses_configured_timeout(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery uses the configured timeout."""
    alive_runner = create_runner_context("timeout-test-runner")

    # Register heartbeat so runner is considered alive
    app_instance.orchestrator.register_runner_heartbeat(
        alive_runner, can_run_atomic_service=False
    )

    invocation_id = create_running_invocation(app_instance, alive_runner, task_value=20)

    # The timeout in config should be used
    timeout_minutes = (
        app_instance.conf.atomic_service_runner_considered_dead_after_minutes
    )
    timeout_seconds = timeout_minutes * 60

    # With the configured timeout, it should NOT be recoverable
    # (because runner has recent heartbeat)
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=timeout_seconds
        )
    )

    # Should NOT be in recovery with full configured timeout
    assert invocation_id not in recoverable

    # But WITH zero timeout (all runners considered dead), it should be recoverable
    recoverable_zero = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )
    assert invocation_id in recoverable_zero


def test_heartbeat_prevents_recovery_timeout(
    app_instance: "Pynenc",
) -> None:
    """Test that regular heartbeats prevent invocations from being recovered."""
    alive_runner = create_runner_context("heartbeat-runner")
    invocation_id = create_running_invocation(app_instance, alive_runner, task_value=21)

    # Register heartbeat
    app_instance.orchestrator.register_runner_heartbeat(
        alive_runner, can_run_atomic_service=False
    )

    # Even with a very long timeout check, should not be recoverable
    # because heartbeat was just registered
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=600.0  # 10 minutes
        )
    )

    assert invocation_id not in recoverable


# ################################################################################### #
# EDGE CASE TESTS
# ################################################################################### #


def test_zero_timeout_marks_all_as_dead(
    app_instance: "Pynenc",
) -> None:
    """Test that zero timeout finds all invocations (edge case)."""
    runner1 = create_runner_context("edge-runner-1")
    runner2 = create_runner_context("edge-runner-2")

    # Create invocations
    inv1 = create_running_invocation(app_instance, runner1, task_value=30)
    inv2 = create_running_invocation(app_instance, runner2, task_value=31)

    # With zero timeout, both should be recoverable
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    assert inv1 in recoverable
    assert inv2 in recoverable


def test_very_large_timeout_marks_none_as_dead(
    app_instance: "Pynenc",
) -> None:
    """Test that very large timeout doesn't recover recent invocations."""
    runner = create_runner_context("edge-runner-large")

    # Register heartbeat so runner is alive
    app_instance.orchestrator.register_runner_heartbeat(
        runner, can_run_atomic_service=False
    )

    invocation_id = create_running_invocation(app_instance, runner, task_value=32)

    # With very large timeout (1 year in seconds), runner is still active
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=365 * 24 * 60 * 60
        )
    )

    # Should not be recoverable (runner is alive)
    assert invocation_id not in recoverable


def test_runner_with_no_invocations_not_recovered(
    app_instance: "Pynenc",
) -> None:
    """Test that runners with no invocations don't cause issues."""
    # Don't create any invocations
    # Just check that recovery doesn't crash
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    # Should be an empty list or not contain anything
    assert isinstance(recoverable, list)


# ################################################################################### #
# CHILD/PARENT RUNNER HIERARCHY TESTS (PPRWorker scenario)
# ################################################################################### #


def test_child_worker_heartbeat_prevents_recovery_by_parent(
    app_instance: "Pynenc",
) -> None:
    """
    Test that child worker heartbeats prevent parent from recovering their invocations.

    This reproduces a production bug where PersistentProcessRunner (parent) was
    recovering invocations owned by PPRWorker (child) even though the child
    was actively sending heartbeats.

    Scenario:
    - Parent runner: PersistentProcessRunner
    - Child runner: PPRWorker (spawned by parent)
    - Child owns RUNNING invocation
    - Child sends heartbeat
    - Parent runs recovery service
    - Expected: Invocation should NOT be recovered
    """
    # Simulate parent/child hierarchy
    parent_runner = create_runner_context("PersistentProcessRunner-parent")
    child_runner = RunnerContext(
        runner_cls="PPRWorker",
        runner_id="PPRWorker-child",
        pid=12345,
        hostname="test-host",
        extra_data={},
    )

    # Both runners register heartbeats
    app_instance.orchestrator.register_runner_heartbeat(
        parent_runner, can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeat(
        child_runner, can_run_atomic_service=False
    )

    # Child owns a RUNNING invocation
    invocation_id = create_running_invocation(
        app_instance, child_runner, task_value=100
    )

    # Parent runs recovery (with normal timeout)
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=600.0  # 10 minutes
        )
    )

    # Child's invocation should NOT be recovered
    assert invocation_id not in recoverable, (
        "Child worker's invocation should NOT be recovered when child has recent heartbeat"
    )


def test_child_worker_without_heartbeat_is_recovered(
    app_instance: "Pynenc",
) -> None:
    """
    Test that child worker invocations ARE recovered when child has no heartbeat.

    This is the correct behavior - if a child worker crashes without sending
    heartbeats, its invocations should be recovered.
    """
    # Parent has heartbeat, child does not
    parent_runner = create_runner_context("PersistentProcessRunner-parent")
    child_runner = create_runner_context("PPRWorker-child-no-heartbeat")

    # Only parent registers heartbeat
    app_instance.orchestrator.register_runner_heartbeat(
        parent_runner, can_run_atomic_service=True
    )
    # Child does NOT register heartbeat (simulating crash before first heartbeat)

    # Child owns a RUNNING invocation
    invocation_id = create_running_invocation(
        app_instance, child_runner, task_value=101
    )

    # Parent runs recovery with zero timeout (all runners considered dead)
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )

    # Child's invocation SHOULD be recovered (no heartbeat from child)
    assert invocation_id in recoverable, (
        "Child worker's invocation SHOULD be recovered when child has no heartbeat"
    )


def test_recovery_checks_invocation_owner_not_parent(
    app_instance: "Pynenc",
) -> None:
    """
    Test that recovery checks the invocation OWNER's heartbeat, not the parent's.

    This verifies the critical behavior: even if the parent runner is alive,
    if the child worker (invocation owner) has no heartbeat, the invocation
    should be recovered.
    """
    parent_runner = create_runner_context("PersistentProcessRunner-alive")
    dead_child = create_runner_context("PPRWorker-dead")
    alive_child = create_runner_context("PPRWorker-alive")

    # Parent and alive_child register heartbeats
    app_instance.orchestrator.register_runner_heartbeat(
        parent_runner, can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeat(
        alive_child, can_run_atomic_service=False
    )
    # dead_child does NOT register heartbeat

    # Create invocations owned by each child
    dead_child_inv = create_running_invocation(app_instance, dead_child, task_value=200)
    alive_child_inv = create_running_invocation(
        app_instance, alive_child, task_value=201
    )

    # Run recovery with short timeout
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.5
        )
    )

    # Only dead child's invocation should be recovered
    assert dead_child_inv in recoverable, "Dead child's invocation should be recovered"
    assert alive_child_inv not in recoverable, (
        "Alive child's invocation should NOT be recovered"
    )
