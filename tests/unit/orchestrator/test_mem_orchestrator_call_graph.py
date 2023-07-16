import pytest
from typing import TYPE_CHECKING

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.exceptions import CycleDetectedError
from pynenc.orchestrator.mem_orchestrator import CallGraph
from pynenc.invocation import DistributedInvocation

if TYPE_CHECKING:
    from tests.unit.conftest import MockPynenc


@pytest.fixture
def invocations(
    mock_base_app: "MockPynenc",
) -> tuple[DistributedInvocation, DistributedInvocation, DistributedInvocation]:
    @mock_base_app.task
    def task0() -> None:
        ...

    @mock_base_app.task
    def task1() -> None:
        ...

    @mock_base_app.task
    def task2() -> None:
        ...

    return (
        DistributedInvocation(Call(task0), None),
        DistributedInvocation(Call(task1), None),
        DistributedInvocation(Call(task2), None),
    )


def test_add_calls_without_cycles(invocations: tuple) -> None:
    """Test that will add an invocation to the graph"""
    graph = CallGraph()
    invocation0, invocation1, invocation2 = invocations

    graph.add_invocation_call(invocation0, invocation1)
    assert graph.edges[invocation0.call_id] == {invocation1.call_id}
    graph.add_invocation_call(invocation1, invocation2)
    assert graph.edges[invocation1.call_id] == {invocation2.call_id}
    graph.add_invocation_call(invocation0, invocation2)
    assert graph.edges[invocation0.call_id] == {
        invocation1.call_id,
        invocation2.call_id,
    }

    for invocation in invocations:
        assert invocation.call_id in graph.call_to_invocation
        assert invocation.invocation_id in graph.call_to_invocation[invocation.call_id]


def test_remove_invocation(invocations: tuple) -> None:
    graph = CallGraph()
    invocation0, invocation1, invocation2 = invocations

    graph.add_invocation_call(invocation0, invocation1)
    graph.add_invocation_call(invocation1, invocation2)

    graph.remove_invocation(invocation1)
    assert invocation1.call_id not in graph.edges
    assert invocation1.call_id not in graph.invocations


def test_get_blocking_invocations(invocations: tuple) -> None:
    graph = CallGraph()
    invocation0, invocation1, invocation2 = invocations

    graph.add_invocation_call(invocation0, invocation1)
    graph.add_invocation_call(invocation1, invocation2)
    graph.add_waiting_for(invocation0, invocation1)

    blocking = graph.get_blocking_invocations(2)
    assert isinstance(blocking, list)
    assert len(blocking) == 1
    assert blocking[0] == invocation1


def test_causes_cycle(invocations: tuple) -> None:
    graph = CallGraph()
    invocation0, invocation1, invocation2 = invocations

    graph.add_invocation_call(invocation0, invocation1)
    graph.add_invocation_call(invocation1, invocation2)

    with pytest.raises(CycleDetectedError) as exc_info:
        graph.add_invocation_call(invocation2, invocation0)

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- test_mem_orchestrator_call_graph.task2()\n"
        "- test_mem_orchestrator_call_graph.task0()\n"
        "- test_mem_orchestrator_call_graph.task1()\n"
        "- back to test_mem_orchestrator_call_graph.task2()"
    )

    assert str(exc_info.value) == expected_error
