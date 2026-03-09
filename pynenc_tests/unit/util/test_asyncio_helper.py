import asyncio

import pytest

from pynenc.util.asyncio_helper import run_task_async, run_task_sync


async def async_add(x: int, y: int) -> int:
    """Sample async function for testing."""
    await asyncio.sleep(0.01)  # Small delay to simulate async work
    return x + y


def sync_add(x: int, y: int) -> int:
    """Sample sync function for testing."""
    return x + y


def test_run_task_with_sync_function() -> None:
    """Test run_task with a synchronous function."""
    result = run_task_sync(sync_add, x=2, y=3)
    assert result == 5


@pytest.mark.asyncio
async def test_run_task_with_async_function() -> None:
    """Test run_task with an asynchronous function."""
    result = await run_task_async(async_add, x=2, y=3)
    assert result == 5  # type: ignore


def test_run_task_with_async_function_in_existing_loop() -> None:
    """Test run_task with an async function when an event loop already exists."""

    async def run_test() -> None:
        # Should return coroutine to await when in event loop
        coro = run_task_async(async_add, x=2, y=3)
        assert asyncio.iscoroutine(coro)
        result = await coro
        assert result == 5  # type: ignore

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        loop.run_until_complete(run_test())
    finally:
        loop.close()
        asyncio.set_event_loop(None)


def test_run_task_with_invalid_args() -> None:
    """Test run_task with invalid arguments raises appropriate errors."""
    with pytest.raises(TypeError):
        run_task_sync(sync_add, invalid_arg=1)  # type: ignore

    with pytest.raises(TypeError):
        run_task_sync(async_add, invalid_arg=1)  # type: ignore


@pytest.mark.asyncio
async def test_run_task_in_async_context() -> None:
    """Test run_task within an async context."""
    result = await run_task_async(async_add, x=2, y=3)
    assert result == 5  # type: ignore

    result = await run_task_async(sync_add, x=2, y=3)
    assert result == 5


def test_run_task_sync_raises_error_in_active_loop() -> None:
    """Test that run_task_sync raises RuntimeError when called from within an active event loop."""

    async def try_run_sync_from_loop() -> None:
        # Attempt to call run_task_sync from within an active event loop
        run_task_sync(async_add, x=2, y=3)  # type: ignore

    loop = asyncio.new_event_loop()
    asyncio.set_event_loop(loop)
    try:
        with pytest.raises(RuntimeError) as excinfo:
            loop.run_until_complete(try_run_sync_from_loop())
        # Verify the error message
        assert "cannot be called from a running event loop" in str(excinfo.value)
    finally:
        loop.close()
        asyncio.set_event_loop(None)
