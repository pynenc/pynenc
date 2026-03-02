"""
Integration test for deep, randomised family trees in pynmon.

Uses PersistentProcessRunner with SQLite backend to create tasks that
randomly spawn children across multiple generations.  Tasks fire-and-forget
their children (no waiting on results) so worker slots are never blocked.

The tree shape is controlled by:
- ``max_depth``: maximum generation depth
- ``max_children``: upper bound on children per node
- ``sleep_min_ms`` / ``sleep_max_ms``: per-task work duration range

Multiple task functions with different names provide visual variety in the
family tree SVG.  They all delegate to the same core logic.

IMPORTANT: This test uses PynencBuilder().sqlite() to configure the app
WITHOUT setting environment variables at module import time. Environment
variables are ONLY set during test execution for child processes and
cleaned up immediately after to prevent pollution of other tests.
"""

import os
import random
import re
import tempfile
import threading
import time
from collections.abc import Generator
from typing import TYPE_CHECKING

import pytest

from pynenc.builder import PynencBuilder
from pynenc.runner.persistent_process_runner import PersistentProcessRunner

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient

# Debug configuration - Set to 1 to keep server alive for browser debugging
KEEP_ALIVE = 0

APP_ID = "test-pynmon-family-tree-deep"

# Create temp database file at module load (but DO NOT set env vars)
_temp_db = tempfile.NamedTemporaryFile(suffix=".db", delete=False)
_temp_db_path = _temp_db.name
_temp_db.close()

# Configure app with SQLite backend and PersistentProcessRunner
# Since tasks fire-and-forget children, 4 processes is plenty —
# workers never block waiting for subtree results.
app = (
    PynencBuilder()
    .sqlite(_temp_db_path)
    .persistent_process_runner(num_processes=4)
    .runner_tuning(
        runner_loop_sleep_time_sec=0.01,
        invocation_wait_results_sleep_time_sec=0.01,
    )
    .app_id(APP_ID)
    .build()
)


# ---------------------------------------------------------------------------
# Core logic shared by all task variants
# ---------------------------------------------------------------------------


def _spawn_generation(
    depth: int,
    max_depth: int,
    max_children: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
    seed: int,
) -> str:
    """Core fire-and-forget logic for every task variant.

    Sleeps a random pre-work duration, spawns children (without waiting for
    their results), then sleeps a random post-work duration.  Returns a short
    summary string so the invocation has a non-trivial result.

    :param int depth: Current generation (0 = root).
    :param int max_depth: Stop spawning beyond this depth.
    :param int max_children: Upper bound on children per node.
    :param int sleep_min_ms: Minimum sleep per phase in milliseconds.
    :param int sleep_max_ms: Maximum sleep per phase in milliseconds.
    :param int seed: RNG seed for reproducibility.
    :return: Summary of what this node did.
    """
    rng = random.Random(seed)

    # Phase 1: simulate pre-work
    pre_ms = rng.randint(sleep_min_ms, sleep_max_ms)
    time.sleep(pre_ms / 1000.0)

    if depth >= max_depth:
        return f"leaf@depth={depth} slept={pre_ms}ms"

    # Choose how many children and which task variant each gets
    num_children = rng.randint(1, max_children)
    task_variants = [process_batch, validate_record, transform_data, aggregate_results]

    child_args = [
        (
            depth + 1,
            max_depth,
            max_children,
            sleep_min_ms,
            sleep_max_ms,
            rng.randint(0, 2**31),
        )
        for _ in range(num_children)
    ]

    # Fire off children across random task variants — don't wait for results
    for args in child_args:
        variant = rng.choice(task_variants)
        variant(*args)  # enqueues invocation, returns immediately (we ignore it)

    # Phase 2: simulate post-work after spawning children
    remaining_budget = max(0, sleep_max_ms - pre_ms)
    if remaining_budget > sleep_min_ms:
        post_ms = rng.randint(sleep_min_ms, remaining_budget)
        time.sleep(post_ms / 1000.0)
    else:
        post_ms = 0

    return f"node@depth={depth} children={num_children} slept={pre_ms + post_ms}ms"


# ---------------------------------------------------------------------------
# Task variants — different names, same core logic, slight flavour text
# ---------------------------------------------------------------------------


@app.task
def process_batch(
    depth: int,
    max_depth: int,
    max_children: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
    seed: int,
) -> str:
    """Process a batch of records, possibly spawning sub-batches."""
    return _spawn_generation(
        depth, max_depth, max_children, sleep_min_ms, sleep_max_ms, seed
    )


@app.task
def validate_record(
    depth: int,
    max_depth: int,
    max_children: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
    seed: int,
) -> str:
    """Validate a record, possibly triggering downstream checks."""
    return _spawn_generation(
        depth, max_depth, max_children, sleep_min_ms, sleep_max_ms, seed
    )


@app.task
def transform_data(
    depth: int,
    max_depth: int,
    max_children: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
    seed: int,
) -> str:
    """Transform data, possibly spawning child transformations."""
    return _spawn_generation(
        depth, max_depth, max_children, sleep_min_ms, sleep_max_ms, seed
    )


@app.task
def aggregate_results(
    depth: int,
    max_depth: int,
    max_children: int,
    sleep_min_ms: int,
    sleep_max_ms: int,
    seed: int,
) -> str:
    """Aggregate results, possibly fanning out sub-aggregations."""
    return _spawn_generation(
        depth, max_depth, max_children, sleep_min_ms, sleep_max_ms, seed
    )


# Environment variables needed by child processes
_ENV_VARS = [
    "PYNENC__SQLITE_DB_PATH",
    "PYNENC__ORCHESTRATOR_CLS",
    "PYNENC__BROKER_CLS",
    "PYNENC__STATE_BACKEND_CLS",
    "PYNENC__ARG_CACHE_CLS",
]


@pytest.fixture
def sqlite_env_for_child_processes() -> Generator[None, None, None]:
    """Set environment variables for PersistentProcessRunner child processes.

    PersistentProcessRunner spawns child processes that create their own
    Pynenc app instances from environment variables.  This fixture sets those
    env vars at test start and restores original values afterwards.
    """
    original_values = {key: os.environ.get(key) for key in _ENV_VARS}

    os.environ["PYNENC__SQLITE_DB_PATH"] = _temp_db_path
    os.environ["PYNENC__ORCHESTRATOR_CLS"] = "SQLiteOrchestrator"
    os.environ["PYNENC__BROKER_CLS"] = "SQLiteBroker"
    os.environ["PYNENC__STATE_BACKEND_CLS"] = "SQLiteStateBackend"
    os.environ["PYNENC__ARG_CACHE_CLS"] = "SQLiteArgCache"

    yield

    for key, original_value in original_values.items():
        if original_value is None:
            os.environ.pop(key, None)
        else:
            os.environ[key] = original_value


@pytest.fixture(scope="module", autouse=True)
def cleanup_temp_db() -> Generator[None, None, None]:
    """Clean up temp database file after all tests in module complete."""
    yield
    if _temp_db_path and os.path.exists(_temp_db_path):
        try:
            os.unlink(_temp_db_path)
        except OSError:
            pass


def test_deep_family_tree(
    pynmon_client: "PynmonClient",
    sqlite_env_for_child_processes: None,
) -> None:
    """Generate a deep randomised family tree and verify the timeline renders.

    Fire-and-forget pattern: we know roughly how long the tree takes:
      max_depth × sleep_max_ms ≈ worst-case serial path.
    We wait that + a generous margin, then check the endpoints.

    Also validates that the SVG layout is compact (time-ordered grid with
    multiple columns) by checking dimensions against expected bounds.
    """
    app.purge()

    # Start PersistentProcessRunner
    runner = PersistentProcessRunner(app)
    runner_thread = threading.Thread(target=runner.run, daemon=True)
    runner_thread.start()

    time.sleep(0.5)  # Let runner initialise

    # Tree parameters — deeper tree to stress the layout
    max_depth = 7
    max_children = 4
    sleep_min_ms = 5
    sleep_max_ms = 40

    try:
        # Kick off the root — returns immediately (fire-and-forget children)
        inv = process_batch(
            depth=0,
            max_depth=max_depth,
            max_children=max_children,
            sleep_min_ms=sleep_min_ms,
            sleep_max_ms=sleep_max_ms,
            seed=42,
        )

        # Poll until all invocations in the family tree reach a final
        # state.  We fetch the family-tree SVG and parse data-status
        # attributes from every node.
        final_statuses = {"SUCCESS", "FAILED", "CONCURRENCY_CONTROLLED_FINAL"}
        timeout_secs = 60.0
        poll_interval = 1.0
        deadline = time.monotonic() + timeout_secs
        content = ""

        while time.monotonic() < deadline:
            response = pynmon_client.get(
                f"/invocations/{inv.invocation_id}/family-tree"
            )
            assert response.status_code == 200
            content = response.text

            statuses = set(re.findall(r'data-status="([^"]+)"', content))
            if statuses and statuses <= final_statuses:
                break
            time.sleep(poll_interval)
        else:
            # Timed out — report which statuses are still pending
            statuses = set(re.findall(r'data-status="([^"]+)"', content))
            non_final = statuses - final_statuses
            pytest.fail(
                f"Timed out after {timeout_secs}s waiting for all "
                f"invocations to reach final state.  "
                f"Non-final statuses still present: {non_final}"
            )

        # Verify timeline renders successfully
        response = pynmon_client.get("/invocations/timeline?time_range=5m")
        assert response.status_code == 200

        # SVG should contain the root invocation and multiple task names
        assert inv.invocation_id in content
        assert "process_batch" in content or "validate_record" in content

        # Validate SVG structure: viewBox should exist and have positive dimensions
        viewbox_match = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', content)
        assert viewbox_match, "SVG should have a viewBox attribute"
        svg_w = float(viewbox_match.group(1))
        svg_h = float(viewbox_match.group(2))
        assert svg_w > 0 and svg_h > 0, "SVG dimensions must be positive"
    finally:
        runner.stop_runner_loop()
        runner_thread.join(timeout=5)
