import asyncio
from collections.abc import Iterable, Sequence

import pytest

from pynenc import Pynenc

# Create a test app instance
app = Pynenc(app_id="test_direct_task")
# Forcing SYNC TASKs, only interested in testing the decorator
app.conf.dev_mode_force_sync_tasks = True


# Simple synchronous direct task
@app.direct_task
def add(x: int, y: int) -> int:
    return x + y


# Async direct task
@app.direct_task
async def async_add(x: int, y: int) -> int:
    await asyncio.sleep(0.01)  # Simulate async work
    return x + y


# Direct task with parallel execution
def generate_pairs(_: dict) -> Iterable[tuple[int, int]]:
    return [(i, i + 1) for i in range(5)]


def sum_results(results: Iterable[int]) -> int:
    return sum(results)


@app.direct_task(parallel_func=generate_pairs, aggregate_func=sum_results)
def parallel_add(x: int, y: int) -> int:
    return x + y


def test_direct_task_sync_basic() -> None:
    """Test that a synchronous direct_task returns the result directly."""
    result = add(3, 4)
    assert isinstance(result, int)
    assert result == 7


@pytest.mark.asyncio
async def test_direct_task_async_basic() -> None:
    """Test that an async direct_task returns the result directly when awaited."""
    result = await async_add(3, 4)
    assert isinstance(result, int)
    assert result == 7


def test_direct_task_sync_parallel() -> None:
    """Test that a synchronous direct_task with parallel execution returns aggregated result."""
    # parallel_add will ignore its arguments and use generate_pairs to create [(0,1), (1,2), (2,3), (3,4), (4,5)]
    # Results: [1, 3, 5, 7, 9] -> sum = 25
    result = parallel_add(999, 999)  # Arguments are ignored due to parallel_func
    assert isinstance(result, int)
    assert result == 25  # Sum of 1 + 3 + 5 + 7 + 9


# Make parallel_add async for this test
@app.direct_task(parallel_func=generate_pairs, aggregate_func=sum_results)
async def async_parallel_add(x: int, y: int) -> int:
    await asyncio.sleep(0.01)  # Simulate async work
    return x + y


@pytest.mark.asyncio
async def test_direct_task_async_parallel() -> None:
    """Test that an async direct_task with parallel execution returns aggregated result."""
    result = await async_parallel_add(
        999, 999
    )  # Arguments ignored due to parallel_func
    assert isinstance(result, int)
    assert result == 25


def dummy_parallel_func(_: dict) -> Iterable[tuple[int, int]]:
    return [(1, 2), (3, 4)]


@app.direct_task(parallel_func=dummy_parallel_func)
def no_aggregate(x: int, y: int) -> int:
    return x + y


def test_direct_task_no_aggregate_func_raises() -> None:
    """Test that parallel execution without an aggregate_func raises an error."""

    with pytest.raises(
        ValueError, match="Aggregation function required for parallel execution"
    ):
        no_aggregate(1, 2)


@app.direct_task
def sync_add(x: int, y: int) -> int:
    return x + y


def test_direct_task_sync_with_dev_mode_force_sync() -> None:
    """Test that direct_task respects dev_mode_force_sync_tasks for synchronous execution."""

    result = sync_add(3, 4)
    assert isinstance(result, int)
    assert result == 7


@app.direct_task
async def sync_async_add(x: int, y: int) -> int:
    await asyncio.sleep(0.01)
    return x + y


@pytest.mark.asyncio
async def test_direct_task_async_with_dev_mode_force_sync() -> None:
    """Test that direct_task respects dev_mode_force_sync_tasks for async execution."""
    result = await sync_async_add(3, 4)
    assert isinstance(result, int)
    assert result == 7


@app.direct_task(
    parallel_batch_size=10,
    retry_for=(ValueError,),
    max_retries=3,
    call_result_cache=True,
)
def add_with_options(x: int, y: int) -> int:
    return x + y


def test_direct_task_with_options() -> None:
    """Test that direct_task works with various task options."""

    result = add_with_options(3, 4)
    assert result == 7


# Direct task with complex parallel execution
def complex_parallel_args(kwargs: dict) -> Sequence[dict]:
    base = kwargs.get("base", 0)
    return [{"base": base + i} for i in range(3)]  # Reduced from 5 for brevity


def complex_aggregate(results: Iterable[list[int]]) -> list[int]:
    return [item for sublist in results for item in sublist]


@app.direct_task(parallel_func=complex_parallel_args, aggregate_func=complex_aggregate)
def complex_parallel_task(base: int = 0) -> list[int]:
    return [base, base + 1, base + 2]


def test_direct_task_complex_parallel() -> None:
    """Test that direct_task works with complex parallel and aggregate functions."""
    result = complex_parallel_task(base=10)
    # complex_parallel_args: [{"base": 10}, {"base": 11}, {"base": 12}]
    # Each call returns [base, base+1, base+2]
    # Results: [[10, 11, 12], [11, 12, 13], [12, 13, 14]] -> flattened: [10, 11, 12, 11, 12, 13, 12, 13, 14]
    expected = [10, 11, 12, 11, 12, 13, 12, 13, 14]
    assert result == expected


def parallel_with_common_data(kwargs: dict) -> tuple[dict, list[dict]]:
    # Extract the large shared data and the array to split
    items = kwargs.get("items", [1, 2, 3])

    # Return common_args (shared data) and param_iter (one task per item)
    return (
        {"shared_data": kwargs["shared_data"]},
        [{"items": [item]} for item in items],
    )


# Simple aggregation function
def aggregate_results(
    results: Iterable[tuple[str, list[int]]]
) -> tuple[str, list[int]]:
    results = list(results)
    concat_items = [item for sublist in results for item in sublist[1]]
    return results[0][0], concat_items


# Task that processes each item with shared data
@app.direct_task(
    parallel_func=parallel_with_common_data, aggregate_func=aggregate_results
)
def process_item_with_shared_data(
    shared_data: str, items: list[int]
) -> tuple[str, list[int]]:
    """Process an item using the shared data."""
    # Simply return a tuple of the first char of shared data and the processed item
    return (shared_data, items)


def test_direct_task_with_common_data() -> None:
    """Test that direct_task properly handles common data with array splitting."""
    # Test with shared data and a list of items
    result = process_item_with_shared_data(shared_data="test_data", items=[1, 2, 3])

    # Each item is processed with the shared data
    # Expected: [('t', 2), ('t', 4), ('t', 6)]
    expected = ("test_data", [1, 2, 3])
    assert result == expected
