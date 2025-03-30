import asyncio
from typing import Any
from unittest.mock import patch

import pytest

from pynenc.call import Call
from pynenc.exceptions import InvocationError, RetryError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc
from tests.util import capture_logs

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_distributed_invocation_instantiation() -> None:
    invocation = add(1, 2)
    assert isinstance(invocation, DistributedInvocation)
    assert isinstance(invocation.call, Call)
    assert invocation.parent_invocation is None


def test_distributed_invocation_to_and_fromjson() -> None:
    invocation = add(1, 2)
    assert invocation == DistributedInvocation.from_json(app, invocation.to_json())


def test_get_final_result_exception_not_final() -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    app.orchestrator._get_invocation_status_mock.return_value = (
        InvocationStatus.REGISTERED
    )
    # Test that it will raise an exception if the invocation is not finished
    with pytest.raises(InvocationError):
        invocation.get_final_result()


@app.task(max_retries=1)
def retry() -> int:
    raise RetryError()


def test_max_retries() -> None:
    app.orchestrator._get_invocation_retries_mock.return_value = 1
    invocation: DistributedInvocation = retry()  # type: ignore

    with capture_logs(app.logger) as log_buffer:
        with pytest.raises(RetryError):
            invocation.run(runner_args={})
        assert "Invocation MAX-RETRY" in log_buffer.getvalue()


def test_reroute_on_running_control() -> None:
    """
    Tests that invocations are rerouted when not authorized to run by concurrency control.
    """
    with patch.object(
        app.orchestrator,
        "is_authorize_to_run_by_concurrency_control",
        return_value=False,
    ) as mock_is_authorized, patch.object(
        app.orchestrator, "reroute_invocations"
    ) as mock_reroute_invocations:
        invocation: DistributedInvocation = add(1, 2)  # type: ignore
        invocation.run()
        mock_is_authorized.assert_called_once()
        mock_reroute_invocations.assert_called_once_with({invocation})


app2 = MockPynenc()


@app2.task
def add2(x: int, y: int) -> int:
    return x + y


@pytest.mark.asyncio
async def test_distributed_async_result_wait_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    mock_base_app = add2.app
    # Create an invocation using the add task (which returns a DistributedInvocation)
    invocation: DistributedInvocation = add2(1, 2)  # type: ignore

    # Simulate status changes: first RUNNING then SUCCESS.
    statuses = [InvocationStatus.RUNNING, InvocationStatus.SUCCESS]

    def status_side_effect(inv: Any) -> InvocationStatus:
        return statuses.pop(0) if statuses else InvocationStatus.SUCCESS

    mock_base_app.orchestrator._get_invocation_status_mock.side_effect = (  # type: ignore
        status_side_effect
    )

    # Patch get_final_result to return a fixed value (e.g. 3)
    monkeypatch.setattr(type(invocation), "get_final_result", lambda self: 3)

    # Patch runner.async_waiting_for_results to simulate a short wait
    async def fake_async_waiting(
        parent: Any, invs: Any, runner_args: Any = None
    ) -> None:
        await asyncio.sleep(0.01)

    monkeypatch.setattr(
        mock_base_app.runner, "async_waiting_for_results", fake_async_waiting
    )

    # Now, calling async_result() should loop until status becomes final, then return 3.
    result = await invocation.async_result()
    assert result == 3


def test_distributed_invocation_getstate() -> None:
    """Test that __getstate__ correctly serializes the distributed invocation."""
    invocation = add(1, 2)  # Create a sample invocation

    # Expected state should contain all instance attributes
    expected_identity = {
        "invocation_id": invocation.invocation_id,
        "call": invocation.call,
        "parent_invocation": None,
    }

    assert invocation.__getstate__()["identity"] == expected_identity  # type: ignore
