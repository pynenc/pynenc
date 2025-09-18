import threading
import time
from typing import TYPE_CHECKING

import pytest

from pynenc import Task
from pynenc.arg_cache import DisabledArgCache
from pynenc.arguments import ArgumentPrintMode
from tests.util import create_test_logger

if TYPE_CHECKING:
    from pynenc import Pynenc


logger = create_test_logger(__name__)


def test_batch_parallelization_overhead(task_process_large_shared_arg: Task) -> None:
    """
    Test that arg_cache enables fast parallel start for large shared arguments.
    Compares batch parallelization with and without cache.
    :param task_process_large_shared_arg: Task for single large arg
    """
    app = task_process_large_shared_arg.app
    if isinstance(app.arg_cache, DisabledArgCache):
        pytest.skip("Skipping test due to disabled arg_cache")

    app.conf.argument_print_mode = ArgumentPrintMode.TRUNCATED
    app.conf.print_arguments = False

    large_data = "x" * 20_000_000  # 20 MB

    # Start runner thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Run batch with cache disabled
    num_tasks = 3
    batch_results_no_cache = batch_process_shared_data(
        app, large_data, num_tasks, use_arg_cache=False
    )
    batch_elapsed_no_cache = list(batch_results_no_cache.values())

    logger.info("Batch with cache DISABLED:")
    for i, e in enumerate(batch_elapsed_no_cache):
        logger.info(f"No cache, batch task {i}: elapsed={e:.3f} seconds")

    # Enable cache and rerun batch
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()
    batch_results_cache = batch_process_shared_data(
        app, large_data, num_tasks, use_arg_cache=True
    )
    batch_elapsed_cache = list(batch_results_cache.values())

    # Cleanup runner
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")

    logger.info("Batch with cache ENABLED:")
    for i, e in enumerate(batch_elapsed_cache):
        logger.info(f"With cache, batch task {i}: elapsed={e:.3f} seconds")

    # Assert at least one cached batch task is faster than the fastest no-cache batch task
    assert min(batch_elapsed_cache) < min(
        batch_elapsed_no_cache
    ), "No cached batch task started faster than no-cache batch task"


def test_arg_cache_effect_on_task_start(task_process_large_shared_arg: Task) -> None:
    """
    Test that arg_cache speeds up task start time for large arguments.
    Disables arg_cache, runs twice, then re-enables and runs twice, comparing elapsed times.
    :param app: Pynenc app instance
    """

    app = task_process_large_shared_arg.app
    if isinstance(app.arg_cache, DisabledArgCache):
        pytest.skip("Skipping test due to disabled arg_cache")

    large_data = "x" * 20_000_000  # 20 MB of data
    backup_arg_cache = app.arg_cache
    app.arg_cache = DisabledArgCache(app)

    # Start runner thread
    runner_thread = threading.Thread(target=lambda: app.runner.run(), daemon=True)
    runner_thread.start()

    # Run twice with cache disabled
    t0 = time.perf_counter()
    start1 = task_process_large_shared_arg(large_data).result
    elapsed1 = start1 - t0
    t1 = time.perf_counter()
    start2 = task_process_large_shared_arg(large_data).result
    elapsed2 = start2 - t1

    app.arg_cache = backup_arg_cache

    # Run twice with cache enabled
    t2 = time.perf_counter()
    start3 = task_process_large_shared_arg(large_data).result
    elapsed3 = start3 - t2
    t3 = time.perf_counter()
    start4 = task_process_large_shared_arg(large_data).result
    elapsed4 = start4 - t3

    # Cleanup
    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
    if runner_thread.is_alive():
        pytest.fail("Runner thread did not terminate within 5 seconds")

    logger.info(f"No cache,    first run: elapsed={elapsed1:.3f} seconds")
    logger.info(f"No cache,   second run: elapsed={elapsed2:.3f} seconds")
    # Already improve as the arguments are not serialized multiple times in the broker and orchestrator
    logger.info(f"With cache,  first run: elapsed={elapsed3:.3f} seconds")
    logger.info(f"With cache, second run: elapsed={elapsed4:.3f} seconds")

    # The second run with cache should be faster than the first run without cache
    assert elapsed4 < elapsed1, "Arg cache did not speed up task start time"


def batch_process_shared_data(
    app: "Pynenc", large_data: str, num_tasks: int, use_arg_cache: bool = True
) -> dict[str, float]:
    """
    Parallelize process_large_shared_arg and return dict of invocation_id to elapsed time.
    Optionally disables arg_cache for the run.
    :param app: Pynenc app instance
    :param large_data: Large shared argument
    :param num_tasks: Number of parallel tasks
    :param use_arg_cache: Whether to use arg_cache (default True)
    :return: Dict of invocation_id to elapsed time (trigger to start)
    """
    import time

    backup_arg_cache = app.arg_cache
    if not use_arg_cache:
        app.arg_cache = DisabledArgCache(app)
    t0 = time.perf_counter()
    invocation_group = app.get_task(
        "tests.integration.apps.combinations.tasks.process_large_shared_arg"
    ).parallelize(param_iter=[{"large_data": large_data} for _ in range(num_tasks)])
    results = {
        inv.invocation_id: inv.result - t0 for inv in invocation_group.invocations
    }
    app.arg_cache = backup_arg_cache
    return results
