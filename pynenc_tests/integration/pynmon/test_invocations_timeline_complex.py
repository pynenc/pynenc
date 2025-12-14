"""
Integration tests for pynmon invocations timeline.

Tests the complete timeline functionality with memory backend,
multiple tasks with different durations, and realistic timing scenarios.
"""

import threading
import time
from typing import TYPE_CHECKING

from pynenc.builder import PynencBuilder
from pynenc.runner.thread_runner import ThreadRunner

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

# Configure app for testing with Memory backend
# Use runner_tuning for faster polling to reduce test time from ~17s to ~5s
app = (
    PynencBuilder()
    .memory()
    .thread_runner(max_threads=8)
    .runner_tuning(
        runner_loop_sleep_time_sec=0.01,
        invocation_wait_results_sleep_time_sec=0.01,
    )
    .app_id("test-pynmon-timeline-complex")
    .build()
)


@app.task
def grandparent_task(family_id: str, num_children: int) -> None:
    """A task that trigger parent tasks."""
    parent_task.logger.info(f"Starting grandparent task {family_id}")
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


def test_complex_timeline(pynmon_client: "PynmonClient") -> None:
    """Test The complex timeline."""
    # Purge any existing data
    app.purge()

    # Start multiple runners - each thread needs its own ThreadRunner instance
    # because ThreadRunner stores thread-specific state (extra_id)
    runner_threads = []
    runners = []
    for _ in range(2):
        # Create a new ThreadRunner instance for each thread
        runner = ThreadRunner(app)
        runners.append(runner)
        runner_thread = threading.Thread(target=runner.run, daemon=True)
        runner_thread.start()
        runner_threads.append(runner_thread)
    time.sleep(0.05)
    try:
        # Trigger different grandparent tasks
        invs_0 = grandparent_task.parallelize([("familyA", 2), ("familyB", 3)])
        time.sleep(0.02)
        invs_1 = grandparent_task.parallelize([("familyC", 4), ("familyD", 1)])
        time.sleep(0.02)
        inv_2 = grandparent_task("familyE", 2)

        # wait for all to complete
        _ = list(invs_0.results)
        _ = list(invs_1.results)
        _ = inv_2.result

        # Test timeline page
        response = pynmon_client.get("/invocations/timeline?time_range=15m")

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
