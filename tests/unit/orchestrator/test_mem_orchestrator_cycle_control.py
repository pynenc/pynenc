from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.exceptions import CycleDetectedError
from pynenc.invocation import DistributedInvocation
from pynenc.orchestrator.mem_orchestrator import MemCycleControl
from tests.conftest import MockPynenc

mock_base_app = MockPynenc()


@mock_base_app.task
def task0() -> None:
    ...


@mock_base_app.task
def task1() -> None:
    ...


@mock_base_app.task
def task2() -> None:
    ...


@pytest.fixture
def invocations() -> (
    tuple[DistributedInvocation, DistributedInvocation, DistributedInvocation]
):
    return (
        DistributedInvocation(Call(task0), None),
        DistributedInvocation(Call(task1), None),
        DistributedInvocation(Call(task2), None),
    )


def test_add_calls_without_cycles(invocations: tuple) -> None:
    """Test that will add an invocation to the graph"""
    invocation0, invocation1, invocation2 = invocations
    graph = MemCycleControl(invocation0.app)

    graph.add_call_and_check_cycles(invocation0, invocation1)
    assert graph.edges[invocation0.call_id] == {invocation1.call_id}
    graph.add_call_and_check_cycles(invocation1, invocation2)
    assert graph.edges[invocation1.call_id] == {invocation2.call_id}
    graph.add_call_and_check_cycles(invocation0, invocation2)
    assert graph.edges[invocation0.call_id] == {
        invocation1.call_id,
        invocation2.call_id,
    }

    for invocation in invocations:
        assert invocation.call_id in graph.call_to_invocation
        assert invocation.invocation_id in graph.call_to_invocation[invocation.call_id]


def test_remove_invocation(invocations: tuple) -> None:
    invocation0, invocation1, invocation2 = invocations
    graph = MemCycleControl(invocation0.app)

    graph.add_call_and_check_cycles(invocation0, invocation1)
    graph.add_call_and_check_cycles(invocation1, invocation2)

    graph.clean_up_invocation_cycles(invocation1)
    assert invocation1.call_id not in graph.edges
    assert invocation1.call_id not in graph.invocations


def test_causes_cycle(invocations: tuple) -> None:
    invocation0, invocation1, invocation2 = invocations
    graph = MemCycleControl(invocation0.app)

    graph.add_call_and_check_cycles(invocation0, invocation1)
    graph.add_call_and_check_cycles(invocation1, invocation2)

    with pytest.raises(CycleDetectedError) as exc_info:
        graph.add_call_and_check_cycles(invocation2, invocation0)

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- test_mem_orchestrator_cycle_control.task2()\n"
        "- test_mem_orchestrator_cycle_control.task0()\n"
        "- test_mem_orchestrator_cycle_control.task1()\n"
        "- back to test_mem_orchestrator_cycle_control.task2()"
    )

    assert str(exc_info.value) == expected_error
