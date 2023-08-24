from typing import TYPE_CHECKING

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

mock_base_app = MockPynenc()


@mock_base_app.task
def dummy_task(x: int, y: int) -> int:
    return x + y


def test_route_task() -> None:
    """Test that the broker will generate an invocation, route it and change status"""

    # basically same test as tests/orchestrator/test_base_orchestrator.py -> test_route_default
    # but calling directly the broker without passing trhough orchestrator

    invocation: DistributedInvocation = mock_base_app.broker.route_call(
        Call(dummy_task, Arguments({"x": 0, "y": 0}))
    )
    assert isinstance(invocation, DistributedInvocation)
    mock_base_app.broker.route_invocation.assert_called_once()
