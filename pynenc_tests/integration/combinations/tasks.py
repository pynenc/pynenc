import array
import random
import string
import time
from time import process_time, sleep
from typing import Any

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType
from pynenc.exceptions import RetryError

mock_app = Pynenc()


@mock_app.task
def sum_task(x: int, y: int) -> int:
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
    sleep_seconds.app.logger.info(f"Sleeping for {seconds} seconds...")
    sleep(seconds)
    return True


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def cpu_intensive_no_conc(
    min_runtime_sec: float, call_id: int = 0
) -> tuple[float, float, float, int]:
    """
    CPU intensive task that runs for at least min_runtime_sec seconds.
    Returns (start_time, end_time, elapsed, iterations performed).
    :param float min_runtime_sec: Minimum runtime in seconds
    :param int call_id: Call identifier for logging
    :return: (start_time, end_time, elapsed, iterations performed)
    """
    invocation_id = cpu_intensive_no_conc.invocation.invocation_id
    cpu_intensive_no_conc.app.logger.info(
        f"INI cpu_intensive_no_conc {min_runtime_sec=} {call_id=} {invocation_id=}"
    )

    start_time = time.time()
    proc_start = process_time()
    arr = array.array("i", [0] * 100)
    arr2 = array.array("i", [0] * 100)
    counter = 0
    seed = int(time.time() * 1000) ^ hash(invocation_id) ^ call_id
    rng = random.Random(seed)
    rand_str = "".join(rng.choices(string.ascii_letters + string.digits, k=128))
    rand_nums = [rng.randint(0, 10000) for _ in range(100)]
    i = 0
    while (elapsed := process_time() - proc_start) < min_runtime_sec:
        idx = i % 100
        arr[idx] = arr[idx] + rand_nums[idx] % 7
        if i % 5 == 0:
            s = rand_str[idx % len(rand_str) :] + rand_str[: idx % len(rand_str)]
            counter += sum(ord(c) for c in s) % 13
        elif i % 5 == 1:
            s = rand_str[::-1]
            counter += sum(ord(c) for c in s) % 17
        elif i % 5 == 2:
            s = rand_str.upper()
            counter += sum(ord(c) for c in s) % 19
        elif i % 5 == 3:
            s = rand_str.lower()
            counter += sum(ord(c) for c in s) % 23
        else:
            counter += arr[idx] % 29
        if i % 3 == 0:
            idx2 = (idx + 1) % 100
            arr2[idx2] = arr[idx] % 10000
            counter += arr2[idx2] % 5
        elif i % 3 == 1:
            idx2 = (idx + 2) % 100
            arr2[idx2] = (arr[idx] + counter) % 10000
        else:
            counter += 1
        i += 1
    arr[0] = counter % 100
    end_time = time.time()
    cpu_intensive_no_conc.app.logger.info(
        f"END  cpu_intensive_no_conc {min_runtime_sec=} {call_id=} {invocation_id=} {elapsed=} iterations={i}"
    )
    return start_time, end_time, elapsed, i


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def distribute_cpu_work(
    min_runtime_sec: float, num_sub_tasks: int
) -> dict[str, tuple[float, float, float, int]]:
    """
    Distribute CPU-intensive work into sub-tasks and parallelize.
    Each sub-task runs for at least min_runtime_sec seconds and returns timing info.
    :param float min_runtime_sec: Minimum runtime per sub-task in seconds
    :param int num_sub_tasks: Number of sub-tasks to parallelize
    :return: Dict of invocation_id to (start_time, end_time, elapsed, iterations)
    """
    invocation_id = distribute_cpu_work.invocation.invocation_id
    distribute_cpu_work.app.logger.info(
        f"INI distribute_cpu_work {min_runtime_sec=} {num_sub_tasks=} {invocation_id=}"
    )
    start_time = process_time()
    app = distribute_cpu_work.app
    app.logger.info(f"Launching {num_sub_tasks} sub-tasks, each for {min_runtime_sec}s")
    chunks = [
        {"min_runtime_sec": min_runtime_sec, "call_id": i} for i in range(num_sub_tasks)
    ]
    invocation_group = cpu_intensive_no_conc.parallelize(chunks)
    results = {inv.invocation_id: inv.result for inv in invocation_group.invocations}
    elapsed = process_time() - start_time
    distribute_cpu_work.app.logger.info(
        f"END distribute_cpu_work {min_runtime_sec=} {num_sub_tasks=} {invocation_id=} {elapsed=}"
    )
    return results


@mock_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def process_large_shared_arg(large_data: str) -> float:
    """
    Return the time when the task starts running (wall-clock).
    :param str large_data: Large shared argument
    :return: Start time in seconds from perf_counter
    """
    import time

    start_time = time.perf_counter()
    process_large_shared_arg.app.logger.debug(
        f"INI process_large_shared_arg invocation_id={process_large_shared_arg.invocation.invocation_id}"
    )
    return start_time
