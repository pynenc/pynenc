from unittest.mock import MagicMock

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import (
    DistributedInvocation,
    DistributedInvocationGroup,
    InvocationStatus,
)
from pynenc_tests.conftest import MockPynenc

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
    # filter final will return both invocations.
    app.orchestrator._mock_filter_final.return_value = [
        invocation0.invocation_id,
        invocation1.invocation_id,
    ]
    app.state_backend._get_result_mock.return_value = -13
    # Both invocations are final so results should be yielded immediately.
    assert list(invocation_group.results) == [-13, -13]


def test_get_pending_results() -> None:
    invocation0: DistributedInvocation = DistributedInvocation(
        call=Call(add, Arguments()), parent_invocation=MagicMock()
    )
    invocation1: DistributedInvocation = add(3, 4)  # type: ignore
    invocation_group: DistributedInvocationGroup = DistributedInvocationGroup(
        task=add, invocations=[invocation0, invocation1]
    )
    # Force pending status for both invocations.
    app.orchestrator._mock_filter_final.return_value = []
    app.orchestrator._get_invocation_status_mock.return_value = InvocationStatus.PENDING
    # Patch waiting methods: orchestrator.waiting_for_results and runner.waiting_for_results.
    app.orchestrator.waiting_for_results = MagicMock()  # type: ignore
    app.runner.waiting_for_results = MagicMock(  # type: ignore
        side_effect=Exception("Abort waiting loop")
    )
    # When we try to evaluate the results iterator, the runner's waiting_for_results should raise.
    with pytest.raises(Exception, match="Abort waiting loop"):
        list(invocation_group.results)
    # Verify that the orchestrator waiting method was called once with the parent's value and the original list.
    app.orchestrator.waiting_for_results.assert_called_once_with(
        invocation0.parent_invocation_id,
        [invocation0.invocation_id, invocation1.invocation_id],
    )
