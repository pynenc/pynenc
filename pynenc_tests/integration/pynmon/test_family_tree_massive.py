"""
Integration test for massive family trees that exceed display limits.

Spawns a wide and deep tree (~150+ nodes) to validate the truncation
behaviour: nodes beyond the configured depth/node cap should render with
a visual indicator and clicking them should re-root the family tree view.

Uses PersistentProcessRunner with SQLite backend.  Tasks fire-and-forget
their children (no waiting on results) so worker slots are never blocked.

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

APP_ID = "test-pynmon-family-tree-massive"

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


def _spawn_chain(
    depth: int,
    max_depth: int,
    children_per_level: int,
    sleep_ms: int,
    seed: int,
) -> str:
    """Core fire-and-forget logic producing a wide, deep tree.

    Each non-leaf node spawns ``children_per_level`` children across
    random task variants.  Short sleeps keep the test fast.

    :param depth: Current generation (0 = root).
    :param max_depth: Stop spawning beyond this depth.
    :param children_per_level: Exact number of children per node.
    :param sleep_ms: Fixed sleep per node in milliseconds.
    :param seed: RNG seed for reproducibility.
    :return: Summary string.
    """
    rng = random.Random(seed)

    time.sleep(sleep_ms / 1000.0)

    if depth >= max_depth:
        return f"leaf@depth={depth}"

    task_variants = [ingest_data, run_pipeline, emit_event, check_result]

    for _ in range(children_per_level):
        variant = rng.choice(task_variants)
        child_seed = rng.randint(0, 2**31)
        variant(depth + 1, max_depth, children_per_level, sleep_ms, child_seed)

    return f"node@depth={depth} children={children_per_level}"


# ---------------------------------------------------------------------------
# Task variants — different names for visual variety in the SVG
# ---------------------------------------------------------------------------


@app.task
def ingest_data(
    depth: int,
    max_depth: int,
    children_per_level: int,
    sleep_ms: int,
    seed: int,
) -> str:
    """Ingest a data chunk, spawning downstream processing."""
    return _spawn_chain(depth, max_depth, children_per_level, sleep_ms, seed)


@app.task
def run_pipeline(
    depth: int,
    max_depth: int,
    children_per_level: int,
    sleep_ms: int,
    seed: int,
) -> str:
    """Run a pipeline stage, spawning child stages."""
    return _spawn_chain(depth, max_depth, children_per_level, sleep_ms, seed)


@app.task
def emit_event(
    depth: int,
    max_depth: int,
    children_per_level: int,
    sleep_ms: int,
    seed: int,
) -> str:
    """Emit an event, triggering downstream handlers."""
    return _spawn_chain(depth, max_depth, children_per_level, sleep_ms, seed)


@app.task
def check_result(
    depth: int,
    max_depth: int,
    children_per_level: int,
    sleep_ms: int,
    seed: int,
) -> str:
    """Check a result, possibly spawning follow-up checks."""
    return _spawn_chain(depth, max_depth, children_per_level, sleep_ms, seed)


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


def test_massive_family_tree_should_truncate_beyond_limits(
    pynmon_client: "PynmonClient",
    sqlite_env_for_child_processes: None,
) -> None:
    """Generate a massive tree and verify truncation indicators appear.

    Tree shape: depth=10, 2 children per node => 2^10 - 1 = 1023 total
    nodes.  With default max_depth=8 and max_nodes=60 the tree MUST be
    truncated.  The SVG should contain "load more" badge elements
    (``<g class="ft-load-more">``) to signal the tree continues.
    """
    app.purge()

    # Start PersistentProcessRunner
    runner = PersistentProcessRunner(app)
    runner_thread = threading.Thread(target=runner.run, daemon=True)
    runner_thread.start()

    time.sleep(0.5)  # Let runner initialise

    # Tree params — deep and wide enough to blow past default limits.
    # depth=10, children=2 => up to 1023 nodes (binary tree)
    max_depth = 10
    children_per_level = 2
    sleep_ms = 5  # Minimal sleep to keep test fast

    try:
        # Kick off the root
        inv = ingest_data(
            depth=0,
            max_depth=max_depth,
            children_per_level=children_per_level,
            sleep_ms=sleep_ms,
            seed=777,
        )

        # Poll until all *displayed* invocations are final.
        # Since the tree is truncated, we only see a subset.
        final_statuses = {"SUCCESS", "FAILED", "CONCURRENCY_CONTROLLED_FINAL"}
        timeout_secs = 120.0
        poll_interval = 2.0
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
            statuses = set(re.findall(r'data-status="([^"]+)"', content))
            non_final = statuses - final_statuses
            pytest.fail(
                f"Timed out after {timeout_secs}s waiting for final state. "
                f"Non-final statuses: {non_final}"
            )

        # ── Verify truncation indicators ──────────────────────────────
        # The SVG must contain "load more" badges for truncated subtrees
        load_more_count = len(re.findall(r'class="ft-load-more"', content))
        assert load_more_count > 0, (
            "Expected load-more indicators in the SVG since the tree "
            f"has ~1023 potential nodes but max_nodes=60. "
            f"SVG length: {len(content)} chars"
        )

        # Count rendered nodes (all are ft-node now, truncated ones also
        # have a separate ft-load-more badge)
        node_count = len(re.findall(r'class="ft-node"', content))
        assert node_count <= 65, f"Expected at most ~60 total nodes, got {node_count}"

        # ── Verify load-more badges have valid expand IDs ─────────────
        expand_ids = re.findall(
            r'class="ft-load-more" data-expand-id="([^"]+)"', content
        )
        assert len(expand_ids) > 0, (
            "Should be able to extract invocation IDs from load-more badges"
        )

        # ── Verify SVG has valid viewBox ──────────────────────────────
        viewbox_match = re.search(r'viewBox="0 0 ([\d.]+) ([\d.]+)"', content)
        assert viewbox_match, "SVG should have a viewBox attribute"
        svg_w = float(viewbox_match.group(1))
        svg_h = float(viewbox_match.group(2))
        assert svg_w > 0 and svg_h > 0, "SVG dimensions must be positive"

        # ── Verify expanding a truncated node returns a larger tree ────
        trunc_id = expand_ids[0]
        resp2 = pynmon_client.get(
            f"/invocations/{inv.invocation_id}/family-tree?expand={trunc_id}"
        )
        assert resp2.status_code == 200
        content2 = resp2.text
        # The expanded tree should have more nodes than the initial tree
        node_count2 = len(re.findall(r'class="ft-node"', content2))
        assert node_count2 > node_count, (
            f"Expanded tree should have more nodes than initial "
            f"({node_count2} vs {node_count})"
        )
        # The expanded tree should still contain the focus invocation
        assert str(inv.invocation_id) in content2, (
            f"Expanded tree should contain the focus invocation {inv.invocation_id}"
        )

        # ── Verify timeline renders ───────────────────────────────────
        response = pynmon_client.get("/invocations/timeline?time_range=5m")
        assert response.status_code == 200
    finally:
        runner.stop_runner_loop()
        runner_thread.join(timeout=10)
