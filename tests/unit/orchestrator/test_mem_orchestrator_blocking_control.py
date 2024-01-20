import pytest

from pynenc.call import Call
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.orchestrator.mem_orchestrator import MemBlockingControl
from tests.conftest import MockPynenc

_mock_base_app = MockPynenc()


@_mock_base_app.task
def task0() -> None:
    ...


@_mock_base_app.task
def task1() -> None:
    ...


@pytest.fixture
def invocations() -> tuple[DistributedInvocation, DistributedInvocation]:
    return (
        DistributedInvocation(Call(task0), None),
        DistributedInvocation(Call(task1), None),
    )


def test_get_blocking_invocations(invocations: tuple) -> None:
    invocation0, invocation1 = invocations
    graph = MemBlockingControl(invocation0.app)

    # graph.add_invocation_call(invocation0, invocation1)
    # graph.add_invocation_call(invocation1, invocation2)
    graph.waiting_for_results(invocation0, [invocation1])

    invocation0.app.orchestrator.get_invocation_status.return_value = (
        InvocationStatus.REGISTERED
    )

    blocking: list[DistributedInvocation] = list(graph.get_blocking_invocations(2))
    assert len(blocking) == 1
    assert blocking[0] == invocation1
