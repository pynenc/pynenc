from time import sleep
from typing import TYPE_CHECKING
from collections.abc import Callable
from unittest.mock import patch

from pynenc.invocation import InvocationStatus
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.conftest import MockPynenc
import pynenc.core_tasks

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


def trigger_core_task(
    app: "Pynenc", task_func: Callable, runner_ctx: RunnerContext
) -> None:
    """Execute a core task by patching get_app_and_runner_ctx to return the test context."""
    with patch.object(
        pynenc.core_tasks,
        "get_app_and_runner_ctx",
        return_value=(app, runner_ctx),
    ):
        task_func()


# ============================================================================
# PENDING Invocation Recovery Tests (recover_pending_invocations)
# ============================================================================


def test_recover_pending_invocations_recovers_stuck_invocations(
    app_instance: "Pynenc",
) -> None:
    """Test that recover_pending_invocations reroutes stuck invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 0.1
    # Bind dummy task to current app instance
    dummy_task.app = app_instance

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        sleep(0.15)  # Wait for timeout

        # Trigger core task
        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_pending_invocations, runner_ctx
        )

        status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        assert status == InvocationStatus.REROUTED
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


def test_recover_pending_invocations_ignores_recent(
    app_instance: "Pynenc",
) -> None:
    """Test that recover_pending_invocations ignores invocations that haven't timed out yet."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 1.0
    dummy_task.app = app_instance

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        inv: DistributedInvocation = dummy_task()  # type: ignore
        app_instance.orchestrator.register_new_invocations([inv])
        app_instance.orchestrator.set_invocation_status(
            inv.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        # Trigger core task immediately
        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_pending_invocations, runner_ctx
        )

        status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        assert status == InvocationStatus.PENDING, (
            "Recent PENDING invocations should not be recovered"
        )
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


def test_recover_pending_invocations_handles_multiple(
    app_instance: "Pynenc",
) -> None:
    """Test that recover_pending_invocations handles multiple stuck invocations."""
    original_timeout = app_instance.conf.max_pending_seconds
    app_instance.conf.max_pending_seconds = 0.1
    dummy_task.app = app_instance

    try:
        runner_ctx = create_runner_context("recovery-runner")
        app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])

        inv1: DistributedInvocation = dummy_task()  # type: ignore
        inv2: DistributedInvocation = dummy_task()  # type: ignore
        inv3: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator.register_new_invocations([inv1, inv2, inv3])
        app_instance.orchestrator.set_invocation_status(
            inv1.invocation_id, InvocationStatus.PENDING, runner_ctx
        )
        app_instance.orchestrator.set_invocation_status(
            inv2.invocation_id, InvocationStatus.PENDING, runner_ctx
        )
        app_instance.orchestrator.set_invocation_status(
            inv3.invocation_id, InvocationStatus.PENDING, runner_ctx
        )

        sleep(0.15)  # Wait for timeout

        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_pending_invocations, runner_ctx
        )

        status1 = app_instance.orchestrator.get_invocation_status(inv1.invocation_id)
        status2 = app_instance.orchestrator.get_invocation_status(inv2.invocation_id)
        status3 = app_instance.orchestrator.get_invocation_status(inv3.invocation_id)

        assert status1 == InvocationStatus.REROUTED
        assert status2 == InvocationStatus.REROUTED
        assert status3 == InvocationStatus.REROUTED
    finally:
        app_instance.conf.max_pending_seconds = original_timeout


# ============================================================================
# RUNNING Invocation Recovery Tests (recover_running_invocations)
# ============================================================================


def test_recover_running_invocations_dead_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that recover_running_invocations reroutes invocations from dead runners."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001
    dummy_task.app = app_instance

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

        sleep(0.1)  # Wait for runner_dead to become inactive

        # Keep runner_alive alive
        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])

        # Trigger core task from alive runner context
        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_running_invocations, runner_alive
        )

        status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
        assert status == InvocationStatus.REROUTED
    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout


def test_recover_running_invocations_ignores_active_runner(
    app_instance: "Pynenc",
) -> None:
    """Test that recover_running_invocations ignores invocations from active runners."""
    runner_ctx = create_runner_context("active-runner")
    app_instance.orchestrator.register_runner_heartbeats([runner_ctx.runner_id])
    dummy_task.app = app_instance

    inv: DistributedInvocation = dummy_task()  # type: ignore
    app_instance.orchestrator.register_new_invocations([inv])

    app_instance.orchestrator.set_invocation_status(
        inv.invocation_id, InvocationStatus.PENDING, runner_ctx
    )
    app_instance.orchestrator.set_invocation_status(
        inv.invocation_id, InvocationStatus.RUNNING, runner_ctx
    )

    trigger_core_task(
        app_instance, pynenc.core_tasks.recover_running_invocations, runner_ctx
    )

    status = app_instance.orchestrator.get_invocation_status(inv.invocation_id)
    assert status == InvocationStatus.RUNNING


def test_recover_running_invocations_multiple(
    app_instance: "Pynenc",
) -> None:
    """Test handling of multiple invocations from dead runners."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001
    dummy_task.app = app_instance

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

        sleep(0.1)

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])

        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_running_invocations, runner_alive
        )

        assert (
            app_instance.orchestrator.get_invocation_status(inv1.invocation_id)
            == InvocationStatus.REROUTED
        )
        assert (
            app_instance.orchestrator.get_invocation_status(inv2.invocation_id)
            == InvocationStatus.REROUTED
        )
        assert (
            app_instance.orchestrator.get_invocation_status(inv3.invocation_id)
            == InvocationStatus.REROUTED
        )
    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout


def test_recover_running_invocations_mixed(
    app_instance: "Pynenc",
) -> None:
    """Test recovery only affects invocations from dead runners, not alive ones."""
    original_timeout = app_instance.conf.runner_considered_dead_after_minutes
    app_instance.conf.runner_considered_dead_after_minutes = 0.001
    dummy_task.app = app_instance

    try:
        runner_alive = create_runner_context("runner-alive")
        runner_dead = create_runner_context("runner-dead")

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])
        app_instance.orchestrator.register_runner_heartbeats([runner_dead.runner_id])

        inv_alive: DistributedInvocation = dummy_task()  # type: ignore
        inv_dead: DistributedInvocation = dummy_task()  # type: ignore

        app_instance.orchestrator.register_new_invocations([inv_alive, inv_dead])

        # inv_alive owned by runner_alive
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.PENDING, runner_alive
        )
        app_instance.orchestrator.set_invocation_status(
            inv_alive.invocation_id, InvocationStatus.RUNNING, runner_alive
        )

        # inv_dead owned by runner_dead
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.PENDING, runner_dead
        )
        app_instance.orchestrator.set_invocation_status(
            inv_dead.invocation_id, InvocationStatus.RUNNING, runner_dead
        )

        sleep(0.1)

        app_instance.orchestrator.register_runner_heartbeats([runner_alive.runner_id])

        trigger_core_task(
            app_instance, pynenc.core_tasks.recover_running_invocations, runner_alive
        )

        assert (
            app_instance.orchestrator.get_invocation_status(inv_alive.invocation_id)
            == InvocationStatus.RUNNING
        )
        assert (
            app_instance.orchestrator.get_invocation_status(inv_dead.invocation_id)
            == InvocationStatus.REROUTED
        )
    finally:
        app_instance.conf.runner_considered_dead_after_minutes = original_timeout
