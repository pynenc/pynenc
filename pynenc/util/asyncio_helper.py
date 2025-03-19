import asyncio
import inspect
from typing import Callable

from pynenc.types import Params, Result


def run_task_sync(
    func: Callable[Params, Result], *args: Params.args, **kwargs: Params.kwargs
) -> Result:
    """
    Run a task synchronously and return its result.

    If the function is asynchronous, a new event loop is created and run to completion.
    This function must not be called from within an active event loop.
    """
    if not inspect.iscoroutinefunction(func):
        return func(*args, **kwargs)
    try:
        # Check if we're in an event loop
        loop = asyncio.get_running_loop()
        if loop.is_running():
            raise RuntimeError(
                "run_task_sync cannot be called from within an active event loop"
            )
    except RuntimeError:
        pass
    return asyncio.run(func(*args, **kwargs))


async def run_task_async(
    func: Callable[Params, Result], *args: Params.args, **kwargs: Params.kwargs
) -> Result:
    """
    Run a task asynchronously and return its result.

    If the function is asynchronous, it is awaited.
    If it is synchronous, it is called directly.
    """
    if not inspect.iscoroutinefunction(func):
        return func(*args, **kwargs)
    return await func(*args, **kwargs)
