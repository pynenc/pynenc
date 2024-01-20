from typing import TYPE_CHECKING

import pytest

from pynenc.call import Call
from pynenc.exceptions import InvocationError, RetryError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

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


def test_max_retries(caplog: "LogCaptureFixture") -> None:
    app.orchestrator.get_invocation_retries.return_value = 1
    invocation: DistributedInvocation = retry()  # type: ignore
    with caplog.at_level("DEBUG"):
        with pytest.raises(RetryError):
            invocation.run(runner_args={})
        assert any("Max retries reached" in record.message for record in caplog.records)
