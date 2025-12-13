"""
Integration tests for pynmon invocations timeline.

Tests the complete timeline functionality with SQLite backend,
multiple tasks with different durations, and realistic timing scenarios.

Key components:
- Multi-runner concurrent execution testing
- Hierarchical task invocation patterns (grandparent -> parent -> child)
- Timeline visualization verification
"""

import os
import tempfile
import threading
import time
from typing import TYPE_CHECKING


from pynenc import Pynenc
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.runner.multi_thread_runner import MultiThreadRunner
from pynenc.runner.process_runner import ProcessRunner
from pynenc.runner.persistent_process_runner import PersistentProcessRunner

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# App ID shared across all app instances - they communicate via SQLite
APP_ID = "test-pynmon-timeline-complex"


def _get_or_create_db_path() -> str:
    """
    Get the SQLite database path from environment or create a new temp file.

    The first process to run this creates a temp file and sets the env var.
    Child processes inherit the env var and use the same path.
    """
    if db_path := os.environ.get("PYNENC__SQLITE_DB_PATH"):
        return db_path
    # First process - create temp file and set env var for SQLite path
    temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
    db_path = temp_db.name
    temp_db.close()
    os.environ["PYNENC__SQLITE_DB_PATH"] = db_path
    # Also set SQLite component classes so child processes use them
    os.environ["PYNENC__ORCHESTRATOR_CLS"] = "SQLiteOrchestrator"
    os.environ["PYNENC__BROKER_CLS"] = "SQLiteBroker"
    os.environ["PYNENC__STATE_BACKEND_CLS"] = "SQLiteStateBackend"
    os.environ["PYNENC__ARG_CACHE_CLS"] = "SQLiteArgCache"
    return db_path


# Initialize database path - will be shared via env var with child processes
_temp_db_path = _get_or_create_db_path()


def create_app() -> Pynenc:
    """Create a new Pynenc app instance using SQLite from environment."""
    # Pynenc() reads PYNENC__* from environment automatically
    return Pynenc(app_id=APP_ID)


# Primary app instance for task registration and test orchestration
app = create_app()


@app.task
def grandparent_task(family_id: str, num_children: int) -> None:
    """A task that triggers parent tasks."""
    grandparent_task.logger.info(f"Starting grandparent task {family_id}")
    time.sleep(0.5)  # Simulate some work
    invs = parent_task.parallelize(
        [(f"{family_id}-parent-{i}", num_children) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def parent_task(family_id: str, num_children: int) -> None:
    """A task that acts as a parent in invocation hierarchy."""
    parent_task.logger.info(f"Starting parent task {family_id}")
    time.sleep(0.25)  # Simulate some work
    invs = child_task.parallelize(
        [(f"{family_id}-child-{i}",) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def child_task(family_id: str) -> None:
    """A task that acts as a child in invocation hierarchy."""
    child_task.logger.info(f"Starting child task {family_id}")
    time.sleep(0.2)  # Simulate some work


def test_multi_runner_timeline(pynmon_client: "PynmonClient") -> None:
    """
    Test the complex timeline with multiple concurrent runners.

    Each runner gets its own app instance to simulate realistic distributed
    deployment where workers are separate processes with independent app instances.
    """
    # Purge any existing data
    app.purge()

    # Start multiple runners - each with its own app instance
    # This simulates real-world deployment where each worker process
    # has an independent app instance connecting to the same SQLite backend
    runner_threads: list[threading.Thread] = []
    runners: list[
        ThreadRunner | ProcessRunner | MultiThreadRunner | PersistentProcessRunner
    ] = []

    for runner_cls in [
        ThreadRunner,
        ProcessRunner,
        MultiThreadRunner,
        PersistentProcessRunner,
    ]:
        # for runner_cls in [MultiThreadRunner]:
        # Create a fresh app instance for each runner
        runner_app = create_app()
        runner = runner_cls(runner_app)  # type: ignore[abstract]
        runners.append(runner)  # type: ignore[arg-type]
        runner_thread = threading.Thread(target=runner.run, daemon=True)
        runner_thread.start()
        runner_threads.append(runner_thread)

    time.sleep(0.1)  # Let runners initialize

    try:
        # Trigger different grandparent tasks using the primary app instance
        invs_0 = grandparent_task.parallelize([("familyA", 2), ("familyB", 3)])
        time.sleep(0.1)  # Stagger start times
        invs_1 = grandparent_task.parallelize([("familyC", 4), ("familyD", 1)])
        time.sleep(0.2)
        inv_2 = grandparent_task("familyE", 2)

        # Wait for all to complete
        _ = list(invs_0.results)
        _ = list(invs_1.results)
        _ = inv_2.result

        # Test timeline page
        response = pynmon_client.get("/invocations/timeline?time_range=5m")

        assert response.status_code == 200
        content = response.text

        # Check that all invocations appear in the SVG graph
        for inv in invs_0.invocations + invs_1.invocations + [inv_2]:
            assert inv.invocation_id in content
    finally:
        # Stop all runner instances
        for runner in runners:
            runner.stop_runner_loop()
        for runner_thread in runner_threads:
            runner_thread.join(timeout=1)
