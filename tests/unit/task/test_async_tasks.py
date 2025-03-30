from typing import Any
from unittest.mock import AsyncMock, MagicMock

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import (
    DistributedInvocation,
    DistributedInvocationGroup,
    InvocationStatus,
)
from tests.conftest import MockPynenc

# Create a test app instance
app = MockPynenc()


# Define a simple task
@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_async_results_empty() -> None:
    """Test that async_results returns an empty list when there are no invocations."""
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[]
    )
    results = [r async for r in group.async_results()]
    assert results == []


@pytest.mark.asyncio
async def test_async_results_final() -> None:
    """Test that async_results returns the final result when all invocations are already final."""
    # Create two invocations using the task decorator.
    invocation0: DistributedInvocation = add(1, 2)  # type: ignore
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    # Force both invocations to be final.
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.SUCCESS
    # ! Patch _mock_filter_final as it is used for performance instead of individual status checks.
    app.orchestrator._mock_filter_final.return_value = [invocation0, invocation1]
    # Patch get_final_result on both invocations (using object.__setattr__ to bypass immutability)
    object.__setattr__(invocation0, "get_final_result", lambda: -13)
    object.__setattr__(invocation1, "get_final_result", lambda: -13)
    results = [r async for r in group.async_results()]
    assert results == [-13, -13]


@pytest.mark.asyncio
async def test_async_results_pending_exception(monkeypatch: pytest.MonkeyPatch) -> None:
    """Test that async_results raises an exception if the waiting function fails."""
    # Create two invocations: one with a dummy Arguments and one from the task decorator.
    invocation0: DistributedInvocation = DistributedInvocation(
        Call(add, Arguments()), parent_invocation=MagicMock()
    )
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    # Force pending status for both invocations.
    app.orchestrator._mock_filter_final.return_value = []
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.PENDING

    async def dummy_wait(parent: Any, invs: Any, runner_args: Any = None) -> None:
        raise Exception("Abort waiting loop")

    monkeypatch.setattr(
        app.runner, "async_waiting_for_results", AsyncMock(side_effect=dummy_wait)
    )
    with pytest.raises(Exception, match="Abort waiting loop"):
        # Exhaust the async_results generator to trigger the wait loop.
        [r async for r in group.async_results()]
