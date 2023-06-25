from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation, InvocationStatus

if TYPE_CHECKING:
    from tests.conftest import MockPynenc


def test_route_task(mock_base_app: "MockPynenc") -> None:
    """Test that the broker will generate an invocation, route it and change status"""

    # basically same test as tests/orchestrator/test_base_orchestrator.py -> test_route_default
    # but calling directly the broker without passing trhough orchestrator

    @mock_base_app.task
    def dummy_task(x: int, y: int) -> int:
        return x + y

    invocation: DistributedInvocation = mock_base_app.broker.route_task(
        dummy_task, {"x": 0, "y": 0}
    )
    assert isinstance(invocation, DistributedInvocation)
    mock_base_app.broker._route_invocation.assert_called_once()
    mock_base_app.orchestrator.set_invocation_status.assert_called_once_with(
        invocation, InvocationStatus.REGISTERED
    )


def test_retrieve_invocation(mock_base_app: "MockPynenc") -> None:
    """Test that will retrieve an invocation and update status to PENDING"""

    @mock_base_app.task
    def dummy_task(x: int, y: int) -> int:
        return x + y

    # mock the abstract methods with a fake invocation
    invocation: DistributedInvocation = DistributedInvocation(
        dummy_task, {"x": 0, "y": 0}
    )
    mock_base_app.broker._retrieve_invocation.return_value = invocation

    retrieved_invocation = mock_base_app.broker.retrieve_invocation()

    mock_base_app.broker._retrieve_invocation.assert_called_once()
    assert invocation == retrieved_invocation

    mock_base_app.orchestrator.set_invocation_status.assert_called_once_with(
        invocation, InvocationStatus.PENDING
    )
