import pytest

from pynenc import Pynenc
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc_tests.conftest import MockPynenc

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


def test_get_blocking_invocations(invocations: tuple, app_instance: "Pynenc") -> None:
    invocation0, invocation1 = invocations
    graph = app_instance.orchestrator.blocking_control  # type: ignore

    # register new invocations in the orchestrator
    app_instance.orchestrator._register_new_invocations([invocation0, invocation1])

    # graph.add_invocation_call(invocation0, invocation1)
    # graph.add_invocation_call(invocation1, invocation2)
    graph.waiting_for_results(invocation0.invocation_id, [invocation1.invocation_id])

    invocation0.app.orchestrator._get_invocation_status_mock.return_value = (
        InvocationStatus.REGISTERED
    )

    blocking: list[str] = list(graph.get_blocking_invocations(2))
    assert len(blocking) == 1
    assert blocking[0] == invocation1.invocation_id
