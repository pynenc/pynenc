import threading
from collections import Counter
from time import time

import pytest

from pynenc import Pynenc, Task


def run_runner_in_thread(app: Pynenc) -> threading.Thread:
    """Helper function to start the runner in a separate thread"""

    def run() -> None:
        app.runner.run()

    thread = threading.Thread(target=run, daemon=True)
    thread.start()
    return thread


@pytest.mark.asyncio
async def test_async_task_execution(task_async_add: Task) -> None:
    """Test execution of an async task"""
    thread = run_runner_in_thread(task_async_add.app)
    invocation = task_async_add(3, 5)
    result = await invocation.async_result()
    assert result == 8
    task_async_add.app.runner.stop_runner_loop()
    thread.join()


@pytest.mark.asyncio
async def test_async_task_waiting(task_async_sleep: Task) -> None:
    """Test an async task that sleeps for a while before completing"""
    thread = run_runner_in_thread(task_async_sleep.app)
    sleep_time = 0.1
    start_time = time()
    invocation = task_async_sleep(sleep_time)
    result = await invocation.async_result()
    elapsed_time = time() - start_time
    assert result is True
    assert elapsed_time >= sleep_time
    task_async_sleep.app.runner.stop_runner_loop()
    thread.join()


@pytest.mark.asyncio
async def test_async_task_failure(task_async_fail: Task) -> None:
    """Test an async task that raises an exception"""
    thread = run_runner_in_thread(task_async_fail.app)
    invocation = task_async_fail()
    with pytest.raises(ValueError, match="Intentional error"):
        await invocation.async_result()
    task_async_fail.app.runner.stop_runner_loop()
    thread.join()


@pytest.mark.asyncio
async def test_async_task_dependency(task_async_get_upper: Task) -> None:
    """Test an async task that depends on another async task's result"""
    thread = run_runner_in_thread(task_async_get_upper.app)
    invocation = task_async_get_upper()
    result = await invocation.async_result()
    assert result == "EXAMPLE"
    task_async_get_upper.app.runner.stop_runner_loop()
    thread.join()


@pytest.mark.asyncio
async def test_async_task_cycle_detection(task_async_get_text: Task) -> None:
    """Test that an async task does not create a cycle"""
    thread = run_runner_in_thread(task_async_get_text.app)
    invocation = task_async_get_text()
    result = await invocation.async_result()
    assert result == "example"
    task_async_get_text.app.runner.stop_runner_loop()
    thread.join()


@pytest.mark.asyncio
async def test_async_group(task_async_add: Task) -> None:
    """Test the parallel execution functionality using async tasks"""
    app = task_async_add.app
    thread = run_runner_in_thread(app)

    # Create parallel invocations
    invocation_group = task_async_add.parallelize(
        ((1, 2), {"x": 3, "y": 4}, task_async_add.args(5, y=6))
    )

    # Wait for all results asynchronously
    results = [r async for r in invocation_group.async_results()]

    # Validate the results
    assert Counter(results) == Counter([3, 7, 11])

    # Stop the runner
    app.runner.stop_runner_loop()
    thread.join()
