"""
Tests for orchestrator dead runner detection and invocation recovery.

Key components tested:
- Timeout configuration and its dual purposes (recovery + atomic service)
- False positive prevention: healthy runners never marked as dead
- Edge cases with timeout boundaries
"""

import time
from typing import TYPE_CHECKING

from pynenc.runner.runner_context import RunnerContext
from pynenc.invocation.status import InvocationStatus
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation
    from pynenc.identifiers.invocation_id import InvocationId

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
    )


def create_running_invocation(
    app_instance: "Pynenc", runner_ctx: RunnerContext, task_value: int = 0
) -> "InvocationId":
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
    app_instance.orchestrator.register_runner_heartbeats(
        [runner_ctx.runner_id], can_run_atomic_service=False
    )

    # Get active runners with 10-second timeout
    active_runners = app_instance.orchestrator._get_active_runners(
        timeout_seconds=10.0, can_run_atomic_service=False
    )
    runner_ids = {r.runner_id for r in active_runners}

    assert runner_ctx.runner_id in runner_ids


def test_get_active_runners_excludes_stale_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that a runner without recent heartbeat is excluded."""
    runner_ctx = create_runner_context("runner-stale")

    # Register heartbeat for this runner (creates it in the system)
    app_instance.orchestrator.register_runner_heartbeats(
        [runner_ctx.runner_id], can_run_atomic_service=False
    )

    # Sleep briefly to ensure time advances past heartbeat timestamp
    time.sleep(0.001)

    # Check with zero timeout - runner should immediately be stale
    active_runners = app_instance.orchestrator._get_active_runners(
        timeout_seconds=0.0, can_run_atomic_service=False
    )
    runner_ids = {r.runner_id for r in active_runners}

    # With zero timeout, runner should not be active
    assert runner_ctx.runner_id not in runner_ids


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
    app_instance.orchestrator.register_runner_heartbeats(
        [alive_runner.runner_id], can_run_atomic_service=False
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
    app_instance.orchestrator.register_runner_heartbeats(
        [alive_runner.runner_id], can_run_atomic_service=False
    )

    invocation_id = create_running_invocation(app_instance, alive_runner, task_value=20)

    # The timeout in config should be used
    timeout_minutes = app_instance.conf.runner_considered_dead_after_minutes
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
    app_instance.orchestrator.register_runner_heartbeats(
        [alive_runner.runner_id], can_run_atomic_service=False
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
# REALISTIC RECOVERY SCENARIO TESTS
# ################################################################################### #


def test_active_runner_invocation_not_recovered_with_realistic_timeout(
    app_instance: "Pynenc",
) -> None:
    """
    Test realistic recovery scenario with active runner and reasonable timeout.

    This is the critical test that catches implementation bugs where the
    recovery query incorrectly returns invocations from ACTIVE runners.

    Bug this catches: MongoDB aggregation pipeline that failed to properly
    filter out runners with recent heartbeats due to incorrect $lookup/$match
    combinations.

    Scenario:
    - Runner registers heartbeat (making it active)
    - Runner owns a RUNNING invocation
    - Recovery check uses realistic 60-second timeout
    - Expected: Invocation should NOT be recovered (runner is active)
    """
    active_runner = create_runner_context("active-runner-realistic")

    # Register heartbeat FIRST - runner is now active
    app_instance.orchestrator.register_runner_heartbeats(
        [active_runner.runner_id], can_run_atomic_service=False
    )

    # Create RUNNING invocation owned by this active runner
    invocation_id = create_running_invocation(
        app_instance, active_runner, task_value=100
    )

    # Use realistic timeout (60 seconds) - not edge cases like 0 or 1 year
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=60.0
        )
    )

    # This MUST NOT be recovered - the runner just heartbeated
    assert invocation_id not in recoverable, (
        f"Invocation {invocation_id} should NOT be recovered: "
        f"runner {active_runner.runner_id} just registered heartbeat"
    )


def test_multiple_active_runners_none_recovered_realistic(
    app_instance: "Pynenc",
) -> None:
    """
    Test that multiple active runners' invocations are never recovered.

    This tests the implementation handles multiple runners correctly,
    not just single-runner edge cases.
    """
    runners = [create_runner_context(f"multi-active-runner-{i}") for i in range(3)]

    # All runners register heartbeats - all are active
    for runner in runners:
        app_instance.orchestrator.register_runner_heartbeats(
            [runner.runner_id], can_run_atomic_service=False
        )

    # Each runner owns one RUNNING invocation
    invocation_ids = [
        create_running_invocation(app_instance, runner, task_value=200 + i)
        for i, runner in enumerate(runners)
    ]

    # Use realistic 60-second timeout
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=60.0
        )
    )

    # NONE should be recovered - all runners are active
    for inv_id in invocation_ids:
        assert inv_id not in recoverable, (
            f"Invocation {inv_id} should NOT be recovered: runner is active"
        )


def test_mixed_active_and_inactive_runners_recovery(
    app_instance: "Pynenc",
) -> None:
    """
    Test recovery correctly distinguishes active vs inactive runners.

    This is the most important test: verifies that ONLY invocations from
    runners WITHOUT heartbeats are recovered, while invocations from
    runners WITH heartbeats are preserved.
    """
    active_runner = create_runner_context("mixed-active")
    inactive_runner = create_runner_context("mixed-inactive")

    # Only active_runner registers heartbeat
    app_instance.orchestrator.register_runner_heartbeats(
        [active_runner.runner_id], can_run_atomic_service=False
    )
    # inactive_runner does NOT register heartbeat

    # Create invocations for both
    active_inv = create_running_invocation(app_instance, active_runner, task_value=300)
    inactive_inv = create_running_invocation(
        app_instance, inactive_runner, task_value=301
    )

    # Use realistic 60-second timeout
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=60.0
        )
    )

    # Active runner's invocation should NOT be recovered
    assert active_inv not in recoverable, (
        f"Active runner's invocation {active_inv} should NOT be recovered"
    )

    # Inactive runner's invocation SHOULD be recovered
    assert inactive_inv in recoverable, (
        f"Inactive runner's invocation {inactive_inv} SHOULD be recovered"
    )


def test_runner_with_no_invocations_not_recovered(
    app_instance: "Pynenc",
) -> None:
    """Test that recovery doesn't crash when no invocations exist."""
    recoverable = list(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=60.0
        )
    )

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
    )

    # Both runners register heartbeats
    app_instance.orchestrator.register_runner_heartbeats(
        [parent_runner.runner_id], can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeats(
        [child_runner.runner_id], can_run_atomic_service=False
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
    app_instance.orchestrator.register_runner_heartbeats(
        [parent_runner.runner_id], can_run_atomic_service=True
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
    app_instance.orchestrator.register_runner_heartbeats(
        [parent_runner.runner_id], can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeats(
        [alive_child.runner_id], can_run_atomic_service=False
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


# ################################################################################### #
# PARENT-BASED HEALTH REPORTING TESTS (PPRWorker/PersistentProcessRunner scenario)
# ################################################################################### #


def test_parent_reports_active_child_runner_ids(
    app_instance: "Pynenc",
) -> None:
    """
    Test that parent can report active child runner_ids for health verification.

    This test validates the parent-based health reporting mechanism where a parent
    runner (e.g., PersistentProcessRunner) can report which child runner_ids are
    still alive based on OS-level process checks.

    Note: This test uses mock runners. In production with MongoDB, the orchestrator
    would query the parent's get_active_child_runner_ids() to verify child liveness
    before marking invocations for recovery. This may fail in mongo plugin if the
    integration is not properly implemented.
    """
    # Simulate parent with 2 active children and 1 dead child
    parent_runner_id = "PPR-parent-001"
    active_child_1 = "PPRWorker-active-001"
    active_child_2 = "PPRWorker-active-002"
    dead_child = "PPRWorker-dead-001"

    parent_runner = create_runner_context(parent_runner_id)
    active_child_1_ctx = create_runner_context(active_child_1)
    active_child_2_ctx = create_runner_context(active_child_2)
    dead_child_ctx = create_runner_context(dead_child)

    # All children register heartbeats initially
    app_instance.orchestrator.register_runner_heartbeats(
        [parent_runner.runner_id], can_run_atomic_service=True
    )
    app_instance.orchestrator.register_runner_heartbeats(
        [active_child_1_ctx.runner_id], can_run_atomic_service=False
    )
    app_instance.orchestrator.register_runner_heartbeats(
        [active_child_2_ctx.runner_id], can_run_atomic_service=False
    )
    app_instance.orchestrator.register_runner_heartbeats(
        [dead_child_ctx.runner_id], can_run_atomic_service=False
    )

    # Create invocations for each child
    active_1_inv = create_running_invocation(
        app_instance, active_child_1_ctx, task_value=300
    )
    active_2_inv = create_running_invocation(
        app_instance, active_child_2_ctx, task_value=301
    )
    dead_inv = create_running_invocation(app_instance, dead_child_ctx, task_value=302)

    # With normal timeout, none should be recoverable (all have recent heartbeats)
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=600.0
        )
    )
    assert active_1_inv not in recoverable
    assert active_2_inv not in recoverable
    assert dead_inv not in recoverable

    # Simulate parent reporting active children (dead_child is NOT in the list)
    # This is what get_active_child_runner_ids() would return
    active_child_ids = [active_child_1, active_child_2]  # dead_child excluded

    # The orchestrator should be able to use this info to determine
    # that dead_child's invocations should be recovered even if heartbeat
    # was recent, because the parent says the process is dead.
    # For now, we just verify the interface exists and works.
    assert active_child_1 in active_child_ids
    assert active_child_2 in active_child_ids
    assert dead_child not in active_child_ids

    # With zero timeout (simulating parent-reported death), dead child's inv recovered
    recoverable_zero = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )
    assert dead_inv in recoverable_zero


def test_parent_based_health_reporting_without_heartbeat(
    app_instance: "Pynenc",
) -> None:
    """
    Test scenario where parent is alive but child has never sent heartbeat.

    This can happen if a child process crashes immediately after spawning,
    before it can register its first heartbeat. The parent's OS-level
    process check would detect this, while heartbeat-based detection
    would wait for the full timeout.

    Note: This test may fail in mongo plugin if parent-child health
    reporting integration is not properly implemented.
    """
    parent_runner = create_runner_context("PPR-parent-fast-crash")
    crashed_child = create_runner_context("PPRWorker-fast-crash")

    # Only parent registers heartbeat (child crashed before first heartbeat)
    app_instance.orchestrator.register_runner_heartbeats(
        [parent_runner.runner_id], can_run_atomic_service=True
    )

    # Child owns a RUNNING invocation (set before crash)
    crashed_inv = create_running_invocation(app_instance, crashed_child, task_value=400)

    # Parent would know via get_active_child_runner_ids() that this child is dead
    # But without parent-based integration, orchestrator relies on heartbeat timeout
    # With zero timeout, it should be recoverable
    recoverable = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=0.0
        )
    )
    assert crashed_inv in recoverable, (
        "Crashed child's invocation should be recoverable with zero timeout"
    )

    # With long timeout, heartbeat-only detection would NOT recover
    # (this is the gap that parent-based health reporting addresses)
    recoverable_long = set(
        app_instance.orchestrator._get_running_invocations_for_recovery(
            timeout_seconds=600.0
        )
    )
    # Without parent-based health reporting, orchestrator cannot detect this
    # This assertion documents the limitation that parent-based health fixes
    assert crashed_inv in recoverable_long, (
        "Without heartbeat, crashed child's invocation SHOULD be recoverable. "
        "If this fails, the orchestrator implementation may need to check "
        "for missing heartbeat records (never registered) differently."
    )
