from unittest.mock import MagicMock

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


def test_no_invocations_results() -> None:
    invocation_group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[]
    )
    assert list(invocation_group.results) == []


def test_get_final_invocations() -> None:
    invocation0: DistributedInvocation = add(1, 2)  # type: ignore
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    invocation_group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    app.orchestrator.get_invocation_status.return_value = InvocationStatus.SUCCESS
    app.state_backend._get_result.return_value = -13
    assert list(invocation_group.results) == [-13, -13]


def test_get_pending_results() -> None:
    invocation0: DistributedInvocation = DistributedInvocation(
        call=Call(add, Arguments()), parent_invocation=MagicMock()
    )
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    invocation_group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    app.orchestrator.get_invocation_status.return_value = InvocationStatus.PENDING
    # it should raise an exception
    app.runner.waiting_for_results.side_effect = Exception("Abort waiting loop")
    # check it raises the exception
    with pytest.raises(Exception, match="Abort waiting loop"):
        list(invocation_group.results)
    app.orchestrator.blocking_control.waiting_for_results.assert_called_once_with(
        invocation0.parent_invocation, [invocation0, invocation1]
    )
