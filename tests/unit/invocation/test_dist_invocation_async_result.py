import asyncio
from typing import Any
from unittest.mock import AsyncMock

import pytest

from pynenc.exceptions import InvocationError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_distributed_async_result_success(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    # Simulate immediate final status.
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.SUCCESS
    # Patch get_final_result on the class (to avoid FrozenInstanceError)
    monkeypatch.setattr(type(invocation), "get_final_result", lambda self: 3)
    # Ensure the runner's async waiting function does nothing.
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

    app.orchestrator._get_invocation_status_mock.side_effect = status_side_effect
    monkeypatch.setattr(type(invocation), "get_final_result", lambda self: 3)
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
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.FAILED

    def raise_error(self: Any) -> None:
        raise InvocationError(self.invocation_id, "Not final")

    monkeypatch.setattr(type(invocation), "get_final_result", raise_error)
    app.runner.async_waiting_for_results = AsyncMock(return_value=None)  # type: ignore
    with pytest.raises(InvocationError, match="Not final"):
        await invocation.async_result()
