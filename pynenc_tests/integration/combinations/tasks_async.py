import asyncio

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType

# Create a real Pynenc app instance
async_app = Pynenc()


@async_app.task
async def async_add(x: int, y: int) -> int:
    """Simple async addition task"""
    await asyncio.sleep(0.001)
    return x + y


@async_app.task
async def async_get_text() -> str:
    """Returns a text asynchronously"""
    await asyncio.sleep(0.001)
    return "example"


@async_app.task
async def async_get_upper() -> str:
    """Gets the text asynchronously and converts it to uppercase"""
    async_get_text.app = (
        async_get_upper.app
    )  # Hack to refer the new app set in conftest
    return (await async_get_text().async_result()).upper()


@async_app.task
async def async_fail() -> None:
    """Async task that always fails"""
    await asyncio.sleep(0.001)
    raise ValueError("Intentional error")


@async_app.task(running_concurrency=ConcurrencyControlType.TASK)
async def async_sleep_seconds(seconds: int) -> bool:
    """Async task that sleeps for the given number of seconds"""
    await asyncio.sleep(seconds)
    return True
