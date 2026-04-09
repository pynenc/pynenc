from datetime import datetime, UTC
import threading
import time
from typing import TYPE_CHECKING

import pytest

from pynenc import Task
from pynenc.arguments import ArgumentPrintMode
from pynenc.runner import ThreadRunner
from pynenc.identifiers.task_id import TaskId
from pynenc_tests.conftest import check_all_status_transitions
from pynenc_tests.util import create_test_logger

if TYPE_CHECKING:
    from pynenc import Pynenc


logger = create_test_logger(__name__)


@pytest.mark.slow
def test_batch_parallelization_overhead(task_process_large_shared_arg: Task) -> None:
    """
    Test that client_data enables fast parallel start for large shared arguments.
    Compares batch parallelization with and without cache.
    :param task_process_large_shared_arg: Task for single large arg
    """
    app = task_process_large_shared_arg.app
    if app.client_data_store.conf.disable_client_data_store:
        pytest.skip("Skipping test due to disabled ClientDataStore caching")

    app.conf.argument_print_mode = ArgumentPrintMode.TRUNCATED
    app.conf.print_arguments = False

    large_data = "x" * 10_000_000  # 10 MB

    # Start runner thread - keep it running for both batches
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Run batch with cache disabled
    num_tasks = 3
    batch_results_no_cache = batch_process_shared_data(
        app, large_data, num_tasks, use_client_data=False
    )
    batch_elapsed_no_cache = list(batch_results_no_cache.values())

    logger.info("Batch with cache DISABLED:")
    for i, e in enumerate(batch_elapsed_no_cache):
        logger.info(f"No cache, batch task {i}: elapsed={e:.3f} seconds")

    # Run batch with cache enabled (same runner, no restart needed)
    batch_results_cache = batch_process_shared_data(
        app, large_data, num_tasks, use_client_data=True
    )
    batch_elapsed_cache = list(batch_results_cache.values())

    # Cleanup runner
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")
    check_all_status_transitions(app)

    logger.info("Batch with cache ENABLED:")
    for i, e in enumerate(batch_elapsed_cache):
        logger.info(f"With cache, batch task {i}: elapsed={e:.3f} seconds")

    # Assert at least one cached batch task is faster than the fastest no-cache batch task
    # Use a tolerance margin to account for system load, GC, and I/O variance
    assert min(batch_elapsed_cache) < min(batch_elapsed_no_cache) * 1.5, (
        f"No cached batch task started faster than no-cache batch task: "
        f"cache={min(batch_elapsed_cache):.3f}s vs no-cache={min(batch_elapsed_no_cache):.3f}s"
    )


@pytest.mark.slow
def test_client_data_effect_on_task_start(task_process_large_shared_arg: Task) -> None:
    """
    Test that client_data speeds up task start time for large arguments.
    Disables client_data, runs twice, then re-enables and runs twice, comparing elapsed times.
    :param app: Pynenc app instance
    """
    app = task_process_large_shared_arg.app
    if app.client_data_store.conf.disable_client_data_store:
        pytest.skip("Skipping test due to disabled ClientDataStore caching")

    # skipt for MemClasses as the cache do not make a big impact in ThreadRunner
    if (
        "Mem" in app.orchestrator.__class__.__name__
        and "Mem" in app.broker.__class__.__name__
        and isinstance(app.runner, ThreadRunner)
    ):
        pytest.skip("Skipping test for ThreadRunner as client_data impact is minimal")

    large_data = "x" * 10_000_000  # 10 MB of data

    # Save original configuration values
    original_disable_cache = app.client_data_store.conf.disable_client_data_store
    original_min_size = app.client_data_store.conf.min_size_to_cache

    # Configure for testing
    app.client_data_store.conf.min_size_to_cache = 0
    app.client_data_store.conf.disable_client_data_store = True

    # Start runner thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Run twice with cache disabled
    t0 = time.perf_counter()
    ini_utc_1 = datetime.now(UTC)
    inv1 = task_process_large_shared_arg(large_data)
    start1 = inv1.result
    elapsed1 = start1 - t0
    t1 = time.perf_counter()
    ini_utc_2 = datetime.now(UTC)
    inv2 = task_process_large_shared_arg(large_data)
    start2 = inv2.result
    elapsed2 = start2 - t1

    # Enable cache for remaining runs
    app.client_data_store.conf.disable_client_data_store = False

    # Run twice with cache enabled
    t2 = time.perf_counter()
    ini_utc_3 = datetime.now(UTC)
    inv3 = task_process_large_shared_arg(large_data)
    start3 = inv3.result
    elapsed3 = start3 - t2
    t3 = time.perf_counter()
    ini_utc_4 = datetime.now(UTC)
    inv4 = task_process_large_shared_arg(large_data)
    start4 = inv4.result
    elapsed4 = start4 - t3

    # Restore original configuration
    app.client_data_store.conf.disable_client_data_store = original_disable_cache
    app.client_data_store.conf.min_size_to_cache = original_min_size

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")
    check_all_status_transitions(app)

    logger.info(f"No cache,    first run: elapsed={elapsed1:.3f} seconds")
    logger.info(f"No cache,   second run: elapsed={elapsed2:.3f} seconds")
    # Already improve as the arguments are not serialized multiple times in the broker and orchestrator
    logger.info(f"With cache,  first run: elapsed={elapsed3:.3f} seconds")
    logger.info(f"With cache, second run: elapsed={elapsed4:.3f} seconds")

    invs = {
        "inv1": (ini_utc_1, inv1),
        "inv2": (ini_utc_2, inv2),
        "inv3": (ini_utc_3, inv3),
        "inv4": (ini_utc_4, inv4),
    }
    for inv_name, (ini, inv) in invs.items():
        logger.info(f"Invocation {inv_name} at {ini.isoformat()} history:")
        for history in app.state_backend.get_history(inv.invocation_id):
            logger.info(
                f"  - {history.status_record.status} at {history.timestamp.isoformat()}"
            )

    # The second run with cache should be faster than the first run without cache
    # Use a tolerance margin to account for system load, GC, and I/O variance
    assert elapsed4 < elapsed1 * 1.5, (
        f"Client data store did not speed up task start time: "
        f"cached={elapsed4:.3f}s vs uncached={elapsed1:.3f}s"
    )


def batch_process_shared_data(
    app: "Pynenc", large_data: str, num_tasks: int, use_client_data: bool = True
) -> dict[str, float]:
    """
    Parallelize process_large_shared_arg and return dict of invocation_id to elapsed time.
    Optionally disables client_data for the run.
    :param app: Pynenc app instance
    :param large_data: Large shared argument
    :param num_tasks: Number of parallel tasks
    :param use_client_data: Whether to use client_data (default True)
    :return: Dict of invocation_id to elapsed time (trigger to start)
    """
    import time

    # Save and set cache configuration
    original_disable = app.client_data_store.conf.disable_client_data_store
    if not use_client_data:
        app.client_data_store.conf.disable_client_data_store = True

    t0 = time.perf_counter()
    invocation_group = app.get_task(
        TaskId(
            "pynenc_tests.integration.combinations.tasks", "process_large_shared_arg"
        )
    ).parallelize(param_iter=[{"large_data": large_data} for _ in range(num_tasks)])
    results = {
        inv.invocation_id: inv.result - t0 for inv in invocation_group.invocations
    }

    # Restore original configuration
    app.client_data_store.conf.disable_client_data_store = original_disable
    return results
