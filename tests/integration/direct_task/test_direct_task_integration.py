import threading

import pytest

from tests.integration.direct_task.tasks_direct import (
    app,
    async_add,
    async_fail,
    async_parallel_add,
    sync_add,
    sync_fail,
    sync_parallel_add,
)


def test_direct_task_sync_execution() -> None:
    """Test basic synchronous direct_task execution with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    # Direct call should return result immediately
    result = sync_add(2, 3)
    assert result == 5, f"Expected 5, got {result}"

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)


@pytest.mark.asyncio
async def test_direct_task_async_execution() -> None:
    """Test basic asynchronous direct_task execution with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    # Direct call should return awaitable result
    result = await async_add(2, 3)
    assert result == 5, f"Expected 5, got {result}"

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)


def test_direct_task_sync_exception() -> None:
    """Test that a synchronous direct_task raises an exception with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    with pytest.raises(ValueError, match="Sync intentional error"):
        sync_fail()

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)


@pytest.mark.asyncio
async def test_direct_task_async_exception() -> None:
    """Test that an asynchronous direct_task raises an exception with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    with pytest.raises(ValueError, match="Async intentional error"):
        await async_fail()

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)


def test_direct_task_sync_parallel() -> None:
    """Test synchronous direct_task with parallel execution with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    # parallel_func generates [(0,1), (1,2), (2,3)] -> [1, 3, 5] -> sum = 9
    result = sync_parallel_add(999, 999)  # Arguments ignored due to parallel_func
    assert result == 9, f"Expected 9, got {result}"

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)


@pytest.mark.asyncio
async def test_direct_task_async_parallel() -> None:
    """Test asynchronous direct_task with parallel execution with default app"""

    def run_in_thread() -> None:
        app.runner.run()

    runner_thread = threading.Thread(target=run_in_thread, daemon=True)
    runner_thread.start()

    # parallel_func generates [(0,1), (1,2), (2,3)] -> [1, 3, 5] -> sum = 9
    result = await async_parallel_add(
        999, 999
    )  # Arguments ignored due to parallel_func
    assert result == 9, f"Expected 9, got {result}"

    app.runner.stop_runner_loop()
    runner_thread.join(timeout=5)
