"""
Integration tests for pynmon invocations timeline with multiple runner types.

Tests the complete timeline functionality with SQLite backend,
multiple runner types (Thread, Process, MultiThread, PersistentProcess),
and hierarchical task invocation patterns.

Key components:
- Multi-runner concurrent execution testing
- Hierarchical task invocation patterns (grandparent -> parent -> child)
- Timeline visualization verification

IMPORTANT: This test uses PynencBuilder().sqlite() to configure the app
WITHOUT setting environment variables at module import time. Environment
variables are ONLY set during test execution for child processes and
cleaned up immediately after to prevent pollution of other tests.
"""

import os
import tempfile
import threading
import time
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from pynenc import Pynenc
from pynenc.builder import PynencBuilder
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.runner.multi_thread_runner import MultiThreadRunner
from pynenc.runner.process_runner import ProcessRunner
from pynenc.runner.persistent_process_runner import PersistentProcessRunner

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 1

# App ID shared across all app instances - they communicate via SQLite
APP_ID = "test-pynmon-timeline-multi-runner"

# Create temp database file at module load (but DO NOT set env vars)
_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

# Configure app using PynencBuilder with explicit SQLite path
# This does NOT use or set any environment variables
app = PynencBuilder().sqlite(_temp_db_path).thread_runner().app_id(APP_ID).build()


@app.task
def grandparent_task(family_id: str, num_children: int) -> None:
    """A task that triggers parent tasks."""
    grandparent_task.logger.info(f"Starting grandparent task {family_id}")
    time.sleep(0.1)
    invs = parent_task.parallelize(
        [(f"{family_id}-parent-{i}", num_children) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def parent_task(family_id: str, num_children: int) -> None:
    """A task that acts as a parent in invocation hierarchy."""
    parent_task.logger.info(f"Starting parent task {family_id}")
    time.sleep(0.05)
    invs = child_task.parallelize(
        [(f"{family_id}-child-{i}",) for i in range(num_children)]
    )
    _ = list(invs.results)


@app.task
def child_task(family_id: str) -> None:
    """A task that acts as a child in invocation hierarchy."""
    child_task.logger.info(f"Starting child task {family_id}")
    time.sleep(0.02)


# List of env vars needed for child processes
_ENV_VARS = [
    "PYNENC__SQLITE_DB_PATH",
    "PYNENC__ORCHESTRATOR_CLS",
    "PYNENC__BROKER_CLS",
    "PYNENC__STATE_BACKEND_CLS",
    "PYNENC__ARG_CACHE_CLS",
]


@pytest.fixture
def sqlite_env_for_child_processes() -> Generator[None, None, None]:
    """
    Set environment variables ONLY during test execution for child processes.

    ProcessRunner and PersistentProcessRunner spawn child processes that
    need to create their own Pynenc app instances. They read configuration
    from environment variables.

    This fixture sets the env vars at test start and cleans them up after.
    """
    # Store original values
    original_values = {key: os.environ.get(key) for key in _ENV_VARS}

    # Set env vars for child processes
    os.environ["PYNENC__SQLITE_DB_PATH"] = _temp_db_path
    os.environ["PYNENC__ORCHESTRATOR_CLS"] = "SQLiteOrchestrator"
    os.environ["PYNENC__BROKER_CLS"] = "SQLiteBroker"
    os.environ["PYNENC__STATE_BACKEND_CLS"] = "SQLiteStateBackend"
    os.environ["PYNENC__ARG_CACHE_CLS"] = "SQLiteArgCache"

    yield

    # Restore original values immediately after test
    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_db() -> Generator[None, None, None]:
    """Clean up temp database file after all tests in module complete."""
    yield
    # Clean up temp database file
    if _temp_db_path and os.path.exists(_temp_db_path):
        try:
            os.unlink(_temp_db_path)
        except OSError:
            pass


def test_multi_runner_timeline(
    pynmon_client: "PynmonClient",
    sqlite_env_for_child_processes: None,
) -> None:
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

    # Helper to create runner app instances with explicit SQLite config
    def create_runner_app() -> Pynenc:
        return (
            PynencBuilder().sqlite(_temp_db_path).thread_runner().app_id(APP_ID).build()
        )

    for runner_cls in [
        ThreadRunner,
        ProcessRunner,
        MultiThreadRunner,
        PersistentProcessRunner,
    ]:
        # Create a fresh app instance for each runner
        runner_app = create_runner_app()
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
