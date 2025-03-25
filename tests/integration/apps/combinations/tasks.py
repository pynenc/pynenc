from time import process_time, sleep, time
from typing import Any

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.exceptions import RetryError

mock_app = Pynenc()


@mock_app.task
def sum(x: int, y: int) -> int:
    return x + y


@mock_app.task
def cycle_start() -> None:
    _ = cycle_end().result


@mock_app.task
def cycle_end() -> None:
    _ = cycle_start().result


@mock_app.task
def raise_exception() -> Any:
    raise ValueError("test")


@mock_app.task
def get_text() -> str:
    return "example"


@mock_app.task
def get_upper() -> str:
    return get_text().result.upper()


@mock_app.task
def direct_cycle() -> str:
    invocation = direct_cycle()
    return invocation.result.upper()


@mock_app.task(max_retries=2)
def retry_once() -> int:
    if retry_once.invocation.num_retries == 0:
        raise RetryError()
    return retry_once.invocation.num_retries


@mock_app.task(running_concurrency=ConcurrencyControlType.TASK)
def sleep_seconds(seconds: int) -> bool:
    sleep(seconds)
    return True


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def cpu_intensive_no_conc(iterations: int, call_id: int = 0) -> float:
    """CPU intensive task that measures its own execution time.
    Returns the elapsed time in seconds."""
    import array

    invocation_id = cpu_intensive_no_conc.invocation.invocation_id
    cpu_intensive_no_conc.app.logger.info(
        f"INI cpu_intensive_no_conc {iterations=} {call_id=} {invocation_id=}"
    )

    start_time = process_time()

    # Keep the original array that works well for GIL contention
    arr = array.array("i", [0] * 100)

    # Add a second small array for more GIL-bound operations
    arr2 = array.array("i", [0] * 100)

    # Add a counter variable to increase interpreter operations
    counter = 0

    # Simple arithmetic operations with predictable cache behavior
    for i in range(iterations):
        # Original part that works well
        idx = i % 100
        arr[idx] = arr[idx] + 1

        # Additional operations to increase GIL-bound work
        if i % 3 == 0:  # Add branching to slow down execution
            idx2 = (idx + 1) % 100
            # Create data dependencies between arrays
            arr2[idx2] = arr[idx] % 10000
            counter += arr2[idx2] % 5
        elif i % 3 == 1:
            idx2 = (idx + 2) % 100
            arr2[idx2] = (arr[idx] + counter) % 10000
        else:
            counter += 1

    # Use counter at the end to prevent compiler optimization
    arr[0] = counter % 100

    elapsed = process_time() - start_time
    cpu_intensive_no_conc.app.logger.info(
        f"END  cpu_intensive_no_conc {iterations=} {call_id=} {invocation_id=} {elapsed=}"
    )
    return elapsed


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def distribute_cpu_work(total_iterations: int, num_sub_tasks: int) -> list[float]:
    """Distribute CPU-intensive work into sub-tasks and parallelize."""
    invocation_id = distribute_cpu_work.invocation.invocation_id
    distribute_cpu_work.app.logger.info(
        f"INI distribute_cpu_work {total_iterations=} {num_sub_tasks=} {invocation_id=}"
    )
    start_time = process_time()
    chunk_size = max(1, total_iterations // num_sub_tasks)
    chunks = [(chunk_size, i) for i in range(num_sub_tasks - 1)] + [
        (total_iterations - chunk_size * (num_sub_tasks - 1),)
    ]
    app = distribute_cpu_work.app
    app.logger.info(
        f"Split {total_iterations} iterations into {num_sub_tasks} sub-tasks"
    )

    invocation_group = cpu_intensive_no_conc.parallelize(chunks)
    results = {}
    for i, chunk_result in enumerate(invocation_group.results):
        results[f"chunk_{i}"] = chunk_result
    elapsed = process_time() - start_time
    distribute_cpu_work.app.logger.info(
        f"END distribute_cpu_work {total_iterations=} {num_sub_tasks=} {invocation_id=} {elapsed=}"
    )
    return list(results.values())


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def process_large_shared_arg(large_data: str, index: int) -> tuple[int, int]:
    """Process a large shared argument and return its length along with the task index.
    This task is designed to test arg_cache efficiency and batch parallelization overhead.
    """
    invocation_id = process_large_shared_arg.invocation.invocation_id
    process_large_shared_arg.app.logger.debug(
        f"INI process_large_shared_arg {index=} {invocation_id=}"
    )

    # Simple processing to avoid adding CPU time that would mask parallelization overhead
    data_len = len(large_data)

    process_large_shared_arg.app.logger.debug(
        f"END process_large_shared_arg {index=} {invocation_id=} len={data_len}"
    )
    return (data_len, index)


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def batch_process_shared_data(
    shared_data: str, num_tasks: int
) -> list[tuple[int, int]]:
    """Orchestrate the parallel processing of a large shared argument with multiple sub-tasks.
    This measures the batch overhead with a single large shared argument."""
    invocation_id = batch_process_shared_data.invocation.invocation_id
    app = batch_process_shared_data.app
    app.logger.info(
        f"INI batch_process_shared_data {num_tasks=} {invocation_id=} data_len={len(shared_data)}"
    )

    # Pre-cache the large data to get a cache key
    app.logger.info("Pre-caching shared data...")
    start_time = time()
    data_cache_key = batch_process_shared_data.app.arg_cache.serialize(shared_data)
    if app.arg_cache.is_cache_key(data_cache_key):
        app.logger.info(f"Pre-cached shared data with key: {data_cache_key}")
    else:
        app.logger.warning("shared_data was not cached and was serialized instead")
    cache_time = time() - start_time
    app.logger.info(
        f"Pre-cached shared data in {cache_time:.3f}s, key={data_cache_key[:20]}..."
    )

    # Now create parameters with the cache key instead of the actual data
    param_list = [(data_cache_key, i) for i in range(num_tasks)]

    # Execute in parallel
    app.logger.info(
        f"Starting parallel processing of {num_tasks} tasks with shared data"
    )

    invocation_group = process_large_shared_arg.parallelize(param_list)
    results = list(invocation_group.results)

    app.logger.info(
        f"END batch_process_shared_data {num_tasks=} {invocation_id=} results={len(results)}"
    )
    return results
