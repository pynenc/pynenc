import asyncio
from time import sleep

from pynenc import Pynenc

# Create a real Pynenc app instance for direct_task tests
config = {
    # "serializer_cls": "JsonSerializer",
    "runner_cls": "ThreadRunner",
    # "orchestrator_cls": "RedisOrchestrator",
    # "broker_cls": "RedisBroker",
    # "state_backend_cls": "RedisStateBackend",
    "cycle_control": False,
    "blocking_control": False,
    # "runner_loop_sleep_time_sec": 0.01,
    # "invocation_wait_results_sleep_time_sec": 0.01,
    # "logging_level": "info",
}
app = Pynenc(app_id="test_direct_task_integration", config_values=config)


@app.direct_task
def sync_add(x: int, y: int) -> int:
    """Simple synchronous addition task"""
    sleep(0.001)  # Simulate some work
    return x + y


@app.direct_task
async def async_add(x: int, y: int) -> int:
    """Simple asynchronous addition task"""
    await asyncio.sleep(0.001)  # Simulate async work
    return x + y


@app.direct_task
def sync_fail() -> None:
    """Synchronous task that raises an exception"""
    sleep(0.001)
    raise ValueError("Sync intentional error")


@app.direct_task
async def async_fail() -> None:
    """Asynchronous task that raises an exception"""
    await asyncio.sleep(0.001)
    raise ValueError("Async intentional error")


@app.direct_task(
    parallel_func=lambda _: [(i, i + 1) for i in range(3)],
    aggregate_func=sum,
)
def sync_parallel_add(x: int, y: int) -> int:
    """Synchronous parallel addition task"""
    sleep(0.001)
    return x + y


@app.direct_task(
    parallel_func=lambda _: [(i, i + 1) for i in range(3)],
    aggregate_func=sum,
)
async def async_parallel_add(x: int, y: int) -> int:
    """Asynchronous parallel addition task"""
    await asyncio.sleep(0.001)
    return x + y
