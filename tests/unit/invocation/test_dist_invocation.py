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
    app.orchestrator.get_invocation_status.return_value = InvocationStatus.REGISTERED
    # Test that it will raise an exception if the invocation is not finished
    with pytest.raises(InvocationError):
        invocation.get_final_result()


@app.task(max_retries=1)
def retry() -> int:
    raise RetryError()


def test_max_retries() -> None:
    app.orchestrator.get_invocation_retries.return_value = 1
    invocation: DistributedInvocation = retry()  # type: ignore

    with capture_logs(app.logger) as log_buffer:
        with pytest.raises(RetryError):
            invocation.run(runner_args={})
        assert "Max retries reached" in log_buffer.getvalue()


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
