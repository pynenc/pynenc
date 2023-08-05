import pytest
from typing import TYPE_CHECKING

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.exceptions import CycleDetectedError
from pynenc.orchestrator.mem_orchestrator import MemBlockingControl
from pynenc.invocation import DistributedInvocation

if TYPE_CHECKING:
    from tests.conftest import MockPynenc


@pytest.fixture
def invocations(
    mock_base_app: "MockPynenc",
) -> tuple[DistributedInvocation, DistributedInvocation]:
    @mock_base_app.task
    def task0() -> None:
        ...

    @mock_base_app.task
    def task1() -> None:
        ...

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

    blocking: list[DistributedInvocation] = list(graph.get_blocking_invocations(2))
    assert len(blocking) == 1
    assert blocking[0] == invocation1
