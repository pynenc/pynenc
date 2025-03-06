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

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_async_no_invocations_results() -> None:
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[]
    )
    results = [r async for r in group.async_results()]
    assert results == []


@pytest.mark.asyncio
async def test_async_final_invocations() -> None:
    # Create two invocations via the task decorator
    invocation0: DistributedInvocation = add(1, 2)  # type: ignore
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    # Force final status for both
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.SUCCESS
    # Patch get_final_result on both invocations via monkeypatch (or directly, if frozen, use object.__setattr__)
    # Here we use object.__setattr__ to bypass immutability:
    object.__setattr__(invocation0, "get_final_result", lambda: -13)
    object.__setattr__(invocation1, "get_final_result", lambda: -13)
    # Now the group's async_results should yield [-13, -13]
    results = [r async for r in group.async_results()]
    assert results == [-13, -13]


@pytest.mark.asyncio
async def test_async_pending_results(monkeypatch: pytest.MonkeyPatch) -> None:
    # Create two invocations; use a dummy Arguments instance.
    invocation0: DistributedInvocation = DistributedInvocation(
        call=Call(add, Arguments()), parent_invocation=MagicMock()
    )
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    # Force pending status for both
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.PENDING

    # Set runner.async_waiting_for_results to an AsyncMock that raises an exception
    async def dummy_wait(
        parent: Any, invocations: Any, runner_args: Any = None
    ) -> None:
        raise Exception("Abort waiting loop")

    app.runner.async_waiting_for_results = AsyncMock(side_effect=dummy_wait)  # type: ignore
    with pytest.raises(Exception, match="Abort waiting loop"):
        # Attempt to exhaust the async_results generator, which should raise the exception.
        [r async for r in group.async_results()]
