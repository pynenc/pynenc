"""
Integration tests for core tasks across all backend combinations.

Tests verify that core recovery tasks properly recover stuck invocations.
"""

import threading

from pynenc import Task
from pynenc import context


def test_status_owner_match_a_runner_id(task_sum: Task) -> None:
    """Test that recover_pending_invocations recovers stuck PENDING invocations."""
    app = task_sum.app

    def run_in_thread() -> None:
        app.runner.run()

    # We are outside a runner, we will get by default an external runner context
    external_runner_ctx = context.get_current_runner_context(app.app_id)

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    TOTAL_INVOCATIONS = 100
    inv_group = task_sum.parallelize([(i, i) for i in range(100)])
    assert len(list(inv_group.results)) == TOTAL_INVOCATIONS

    # Get all the available runners
    active_runner_ids = {r.runner_id for r in app.orchestrator.get_active_runners()}

    # Verify that every single status is owned by an active runner
    # What does this matters?
    # If an invocation is owned by some id that doesn't match an actual runner
    # The running invocation recovery service is going to pick it up and reroute it
    # pynenc/core_tasks.recover_running_invocations
    for inv in inv_group.invocations:
        for history_record in app.state_backend.get_history(inv.invocation_id):
            assert (
                history_record.runner_context_id in active_runner_ids
                or history_record.runner_context_id == external_runner_ctx.runner_id
            ), (
                f"Invocation {inv.invocation_id} has history with runner_context_id "
                f"{history_record.runner_context_id} which is not in active runners. "
                f"This means a child runner started executing before being properly registered. "
                f"Ensure BaseRunner._register_new_child_runner_context() is called before "
                f"any invocations are executed in new runner instances (check persistent_process_main, "
                f"multi_thread_runner, process_runner initialization)."
            )

    app.runner.stop_runner_loop()
    thread.join()
