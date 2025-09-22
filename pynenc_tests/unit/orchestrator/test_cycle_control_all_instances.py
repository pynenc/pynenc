import pytest

from pynenc import Pynenc
from pynenc.call import Call
from pynenc.exceptions import CycleDetectedError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc_tests.conftest import MockPynenc

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


def test_add_calls_without_cycles(invocations: tuple, app_instance: "Pynenc") -> None:
    """Test that will add an invocation to the graph"""
    invocation0, invocation1, invocation2 = invocations

    # register new invocations in the orchestrator
    app_instance.orchestrator._register_new_invocations(
        [invocation0, invocation1, invocation2]
    )

    graph = app_instance.orchestrator.cycle_control  # type: ignore
    # REMOVE THIS AND MAKE TEST GLOBAL
    graph = app_instance.orchestrator.cycle_control

    graph.add_call_and_check_cycles(invocation0, invocation1)
    assert set(graph.get_callees(invocation0.call_id)) == {invocation1.call_id}
    graph.add_call_and_check_cycles(invocation1, invocation2)
    assert set(graph.get_callees(invocation1.call_id)) == {invocation2.call_id}
    graph.add_call_and_check_cycles(invocation0, invocation2)
    assert set(graph.get_callees(invocation0.call_id)) == {
        invocation1.call_id,
        invocation2.call_id,
    }


def test_causes_cycle(invocations: tuple, app_instance: "Pynenc") -> None:
    invocation0, invocation1, invocation2 = invocations
    graph = app_instance.orchestrator.cycle_control  # type: ignore

    # register new invocations in the orchestrator
    app_instance.orchestrator._register_new_invocations(
        [invocation0, invocation1, invocation2]
    )

    graph.add_call_and_check_cycles(invocation0, invocation1)
    graph.add_call_and_check_cycles(invocation1, invocation2)

    with pytest.raises(CycleDetectedError) as exc_info:
        graph.add_call_and_check_cycles(invocation2, invocation0)

    expected_error = (
        "A cycle was detected: Cycle detected:\n"
        "- Call(task=test_cycle_control_all_instances.task2, arguments=<no_args>)\n"
        "- Call(task=test_cycle_control_all_instances.task0, arguments=<no_args>)\n"
        "- Call(task=test_cycle_control_all_instances.task1, arguments=<no_args>)\n"
        "- back to Call(task=test_cycle_control_all_instances.task2, arguments=<no_args>)"
    )

    assert str(exc_info.value) == expected_error


def test_remove_invocation(invocations: tuple, app_instance: "Pynenc") -> None:
    invocation0, invocation1, invocation2 = invocations
    graph = app_instance.orchestrator.cycle_control  # type: ignore

    # register new invocations in the orchestrator
    app_instance.orchestrator._register_new_invocations(
        [invocation0, invocation1, invocation2]
    )

    graph.add_call_and_check_cycles(invocation0, invocation1)
    graph.add_call_and_check_cycles(invocation1, invocation2)

    # This will not clean it up because the invocations are still non-final
    graph.clean_up_invocation_cycles(invocation1.invocation_id)
    assert invocation1.call_id in set(graph.get_callees(invocation0.call_id))

    # Let's mark invocation1 as final and try the clean up again
    app_instance.orchestrator._set_invocation_status(
        invocation1.invocation_id, InvocationStatus.SUCCESS
    )
    graph.clean_up_invocation_cycles(invocation1.invocation_id)
    assert invocation1.call_id not in set(graph.get_callees(invocation0.call_id))
