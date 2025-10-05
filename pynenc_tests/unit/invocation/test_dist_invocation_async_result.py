import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pynenc.exceptions import InvocationError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc_tests.conftest import MockPynenc

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_distributed_async_result_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    app.orchestrator.get_invocation_status.return_value = InvocationStatus.SUCCESS
    # Patch get_final_result directly on the invocation mock
    invocation.get_final_result = lambda self=None: 3  # type: ignore
    app.runner.async_waiting_for_results = AsyncMock(return_value=None)  # type: ignore
    result = await invocation.async_result()
    assert result == 3


@pytest.mark.asyncio
async def test_distributed_async_result_wait_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    # Simulate a waiting loop: first status RUNNING, then SUCCESS.
    statuses = [InvocationStatus.RUNNING, InvocationStatus.SUCCESS]

    def status_side_effect(inv: Any) -> InvocationStatus:
        del inv
        return statuses.pop(0) if statuses else InvocationStatus.SUCCESS

    app.orchestrator.get_invocation_status.side_effect = status_side_effect
    invocation.get_final_result = lambda self=None: 3  # type: ignore
    # Use an async wait that sleeps briefly.
    app.runner.async_waiting_for_results = AsyncMock(  # type: ignore
        side_effect=lambda parent, invs, runner_args=None: asyncio.sleep(0.01)
    )
    result = await invocation.async_result()
    assert result == 3


@pytest.mark.asyncio
async def test_distributed_async_result_failure(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    # Force FAILED status.
    app.orchestrator.get_invocation_status.return_value = InvocationStatus.FAILED

    def raise_error(self: Any = None) -> None:
        raise InvocationError(invocation.invocation_id, "Not final")

    invocation.get_final_result = raise_error  # type: ignore
    app.runner.async_waiting_for_results = AsyncMock(return_value=None)  # type: ignore
    with pytest.raises(InvocationError, match="Not final"):
        await invocation.async_result()
    with pytest.raises(InvocationError, match="Not final"):
        await invocation.async_result()
