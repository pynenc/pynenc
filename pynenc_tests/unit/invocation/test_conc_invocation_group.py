import pytest

from pynenc.invocation import ConcurrentInvocationGroup
from pynenc_tests.conftest import MockPynenc

app = MockPynenc()
app.conf.dev_mode_force_sync_tasks = True


# A task that simply adds two numbers.
@app.task
def add(x: int, y: int) -> int:
    return x + y


# Fixture creating a group with two concurrent invocations.
@pytest.fixture
def concurrent_group() -> ConcurrentInvocationGroup:
    # Each call to 'add' returns a ConcurrentInvocation
    inv1 = add(1, 2)  # Expected result: 3
    inv2 = add(3, 4)  # Expected result: 7
    return ConcurrentInvocationGroup(task=add, invocations=[inv1, inv2])  # type: ignore


# Test the synchronous results property.
def test_group_sync_results(concurrent_group: ConcurrentInvocationGroup) -> None:
    results = list(concurrent_group.results)
    assert results == [3, 7]


# Test the async_results generator.
@pytest.mark.asyncio
async def test_group_async_results(concurrent_group: ConcurrentInvocationGroup) -> None:
    results = [r async for r in concurrent_group.async_results()]
    assert results == [3, 7]


# Test that an empty group produces an empty async generator.
@pytest.mark.asyncio
async def test_group_async_results_empty() -> None:
    from pynenc.invocation import ConcurrentInvocationGroup

    group: ConcurrentInvocationGroup = ConcurrentInvocationGroup(
        task=add, invocations=[]
    )
    results: list[int] = [r async for r in group.async_results()]
    assert results == []
