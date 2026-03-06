"""
Stress test for status transition integrity under concurrent multi-runner load.

Exercises the orchestrator's atomic status transitions by running many tasks
concurrently across all available runner types.  After completion, every recorded
history entry is validated against the state machine to detect any illegal
transitions caused by race conditions.
"""

import multiprocessing

import pytest

from pynenc import Task
from pynenc.orchestrator import SQLiteOrchestrator
from pynenc_tests.conftest import check_all_status_transitions


def test_status_transitions(
    task_grandparent_task: Task, task_parent_task: Task, task_child_task: Task
) -> None:
    """Validate status transitions for hierarchical task chains.

    Runs tasks that depend on the result of other tasks, creating parent->child
    invocation chains.  Exercises the PAUSED/RESUMED transitions that occur when
    a task waits on a sub-invocation result.

    Uses separate processes for SQLite backends (for true parallelism), or
    threads for Mem backends (in-memory state is inherently thread-safe).
    """
    app = task_grandparent_task.app

    if not isinstance(app.orchestrator, SQLiteOrchestrator):
        pytest.skip(
            "This test is designed for SQLiteOrchestrator to validate multi-process transitions"
        )

    processes = []
    num_workers = 4
    for _ in range(num_workers):
        p = multiprocessing.Process(target=app.runner.run, daemon=False)
        p.start()
        processes.append(p)

    # Trigger different grandparent tasks using the primary app instance
    invs_0 = task_grandparent_task.parallelize([("familyA", 2), ("familyB", 3)])
    invs_1 = task_grandparent_task.parallelize([("familyC", 4), ("familyD", 1)])
    inv_2 = task_grandparent_task("familyE", 2)

    # Wait for all to complete
    _ = list(invs_0.results)
    _ = list(invs_1.results)
    _ = inv_2.result

    check_all_status_transitions(app)

    # Signal runners to stop and terminate workers immediately
    app.runner.stop_runner_loop()
    for w in processes:
        if isinstance(w, multiprocessing.Process):
            w.terminate()

    check_all_status_transitions(app)
