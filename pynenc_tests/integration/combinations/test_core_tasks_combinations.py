"""
Integration tests for core tasks across all backend combinations.

Tests verify that core recovery tasks properly recover stuck invocations.
"""

import os
from unittest.mock import patch
import threading
from time import sleep

import pytest

from pynenc import Task
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.runner_context import RunnerContext
from pynenc_tests.conftest import check_all_status_transitions


@pytest.fixture
def dead_runner_ctx() -> RunnerContext:
    return RunnerContext(
        runner_cls="DeadRunner",
        runner_id="dead-runner-123",
        pid=99999,
        hostname="dead-host",
    )


def test_recover_pending_invocations(
    task_sum: Task, dead_runner_ctx: RunnerContext
) -> None:
    """Test that recover_pending_invocations recovers stuck PENDING invocations."""
    app = task_sum.app
    app.conf.max_pending_seconds = 0.2
    app.conf.recover_pending_invocations_cron = "* * * * *"
    app.conf.atomic_service_interval_minutes = 0.1
    with patch.dict(
        os.environ,
        {
            "PYNENC__MAX_PENDING_SECONDS": str(app.conf.max_pending_seconds),
            "PYNENC__RECOVER_PENDING_INVOCATIONS_CRON": app.conf.recover_pending_invocations_cron,
            "PYNENC__ATOMIC_SERVICE_INTERVAL_MINUTES": str(
                app.conf.atomic_service_interval_minutes
            ),
        },
    ):

        def run_in_thread() -> None:
            app.runner.run()

        # Register a runner heartbeat to avoid dead runnner recovery interference
        # Otherwise, the pending recovery will requeue, the worker will re-run it,
        # and a running recovery may rerouted if the dead runner never seen a hearthbeat
        app.orchestrator.register_runner_heartbeats(
            [app.runner.runner_context.runner_id]
        )

        # Create a stuck invocation in PENDING state with a dead runner
        stuck_invocation: DistributedInvocation = DistributedInvocation.isolated(
            Call(task_sum, task_sum.args(1, 2))
        )
        app.orchestrator.register_new_invocations([stuck_invocation])
        app.orchestrator.set_invocation_status(
            stuck_invocation.invocation_id, InvocationStatus.PENDING, dead_runner_ctx
        )

        # wait to ensure max_pending_seconds is exceeded
        sleep(app.conf.max_pending_seconds + 0.01)

        # Start runner in background
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

        sleep(1)  # Let runner initialize and perform recovery

        # Verify the stuck invocation was recovered
        history = app.state_backend.get_history(stuck_invocation.invocation_id)
        assert InvocationStatus.PENDING_RECOVERY in [
            h.status_record.status for h in history
        ]

        app.runner.stop_runner_loop()
        check_all_status_transitions(app)


def test_recover_running_invocations(
    task_sum: Task, dead_runner_ctx: RunnerContext
) -> None:
    """Test that recover_running_invocations recovers RUNNING invocations from dead runners."""
    app = task_sum.app
    app.conf.max_pending_seconds = 0.2
    app.conf.recover_running_invocations_cron = "* * * * *"
    app.conf.atomic_service_interval_minutes = 0.1
    with patch.dict(
        os.environ,
        {
            "PYNENC__MAX_PENDING_SECONDS": str(app.conf.max_pending_seconds),
            "PYNENC__RECOVER_RUNNING_INVOCATIONS_CRON": app.conf.recover_running_invocations_cron,
            "PYNENC__ATOMIC_SERVICE_INTERVAL_MINUTES": str(
                app.conf.atomic_service_interval_minutes
            ),
        },
    ):

        def run_in_thread() -> None:
            app.runner.run()

        # Create a stuck invocation in RUNNING state with a dead runner
        stuck_invocation: DistributedInvocation = DistributedInvocation.isolated(
            Call(task_sum, task_sum.args(1, 2))
        )
        app.orchestrator.register_new_invocations([stuck_invocation])
        app.orchestrator.set_invocation_status(
            stuck_invocation.invocation_id, InvocationStatus.PENDING, dead_runner_ctx
        )
        app.orchestrator.set_invocation_status(
            stuck_invocation.invocation_id, InvocationStatus.RUNNING, dead_runner_ctx
        )

        # Start runner in background
        thread = threading.Thread(target=run_in_thread, daemon=True)
        thread.start()

        sleep(1)  # Let runner initialize and perform recovery

        # Verify the stuck invocation was recovered
        history = app.state_backend.get_history(stuck_invocation.invocation_id)
        assert InvocationStatus.RUNNING_RECOVERY in [
            h.status_record.status for h in history
        ]

        app.runner.stop_runner_loop()
        check_all_status_transitions(app)
