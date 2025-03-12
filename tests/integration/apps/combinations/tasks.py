from time import sleep
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
def cpu_intensive_no_conc(iterations: int) -> float:
    """CPU intensive task that measures its own execution time.
    Returns the elapsed time in seconds."""
    import array
    from time import process_time  # More precise than time()

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

    return process_time() - start_time


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def distribute_cpu_work(total_iterations: int, num_sub_tasks: int) -> list[float]:
    """Distribute CPU-intensive work into sub-tasks and parallelize."""
    chunk_size = max(1, total_iterations // num_sub_tasks)
    chunks = [(chunk_size,) for _ in range(num_sub_tasks - 1)] + [
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
    return list(results.values())
