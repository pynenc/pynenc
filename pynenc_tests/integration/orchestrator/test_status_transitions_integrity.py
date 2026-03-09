"""
Status transition integrity test under concurrent runner load.

Validates every recorded history entry against the state machine after running
hierarchical task chains (grandparent -> parent -> child) to exercise
PAUSED/RESUMED transitions that occur when a task waits on sub-invocation results.

Uses PersistentProcessRunner to exercise cross-process status transitions.
The ``_subprocess_config`` registry in ``Pynenc`` ensures child processes share
the parent's SQLite database path and component classes, even when the ``spawn``
start method re-imports modules.

Key components:
- test_status_transitions: Validates status history for hierarchical task chains
"""

import os
import tempfile
import threading
from collections.abc import Generator

import pytest

from pynenc import PynencBuilder
from pynenc_tests.conftest import check_all_status_transitions

# Create temp database file at module load (but DO NOT set env vars)
_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

# ThreadRunner will always keep the status consistency due to the GIL
# Use other runner that runs in different processes so we increase conflicts
# ! IMPORTANT: set more runners than waiting tasks to avoid deadlocs
app = (
    PynencBuilder()
    .sqlite(_temp_db_path)
    .persistent_process_runner(num_processes=20)
    .app_id("test-status-integrity")
    .build()
)


@app.task
def grandparent_task(family_id: str, num_children: int) -> None:
    """A task that triggers parent tasks."""
    grandparent_task.logger.info(f"Starting grandparent task {family_id}")
    invs = parent_task.parallelize(
        [(f"{family_id}-parent-{i}", num_children) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def parent_task(family_id: str, num_children: int) -> None:
    """A task that acts as a parent in invocation hierarchy."""
    parent_task.logger.info(f"Starting parent task {family_id}")
    invs = child_task.parallelize(
        [(f"{family_id}-child-{i}",) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def child_task(family_id: str) -> None:
    """A task that acts as a child in invocation hierarchy."""
    child_task.logger.info(f"Starting child task {family_id}")


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_db() -> Generator[None, None, None]:
    """Clean up temp database file after all tests in module complete."""
    yield
    if _temp_db_path and os.path.exists(_temp_db_path):
        try:
            os.unlink(_temp_db_path)
        except OSError:
            pass


def test_status_transitions() -> None:
    """Validate status transitions for hierarchical task chains.

    Runs grandparent -> parent -> child task hierarchy exercising the
    PAUSED/RESUMED transitions that occur when a task waits on sub-invocation
    results. After completion, validates every recorded history entry against
    the state machine.
    """
    app.purge()

    worker = threading.Thread(target=app.runner.run, daemon=True)
    worker.start()

    try:
        # Trigger different grandparent tasks using the primary app instance
        invs_0 = grandparent_task.parallelize([("familyA", 2), ("familyB", 3)])
        invs_1 = grandparent_task.parallelize([("familyC", 4), ("familyD", 1)])
        inv_2 = grandparent_task("familyE", 2)

        # Wait for all to complete
        _ = list(invs_0.results)
        _ = list(invs_1.results)
        _ = inv_2.result

        check_all_status_transitions(app)
    finally:
        app.runner.stop_runner_loop()
