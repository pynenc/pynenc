from typing import TYPE_CHECKING

from pynenc.conf import SingleInvocation
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from tests.conftest import MockPynenc


mock_base_app = MockPynenc()


@mock_base_app.task
def add(x: int, y: int) -> int:
    return x + y


def test_route_default() -> None:
    """Test that the orchestrator will route the task by default

    If there are no options:
     - The orchestrator will forward the task to the broker
     - The broker should return a new Invocation and report the change of status to the orchestrator
    """
    invocation = add(1, 3)
    assert isinstance(invocation, DistributedInvocation)
    # test that app.broker.route_invocation (MockBroker.route_invocation) has been called
    mock_base_app.broker.route_invocation.assert_called_once()
    # test that app.orchestrator.set_invocation_status (MockBaseOrchestrator.set_invocation_status)
    # has been called with (result, InvocationStatus.REGISTERED)
    mock_base_app.orchestrator._set_invocation_status.assert_called_once_with(
        invocation, InvocationStatus.REGISTERED
    )


@mock_base_app.task(single_invocation=SingleInvocation())
def add_single_inv(x: int, y: int) -> int:
    return x + y


def test_single_invocation(mock_base_app: "MockPynenc") -> None:
    """Test that the when `task.options.single_invocation` is set the orchestrator
    will only route the task if do not exists a Pending instance
    """
    # Get existing invocation doesn't find any pending match
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter([])
    first_invocation = add_single_inv(1, 3)
    # Return previous as a pending match
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter(
        [first_invocation]
    )
    next_invocation = add_single_inv(1, 3)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    # Back to no match, generates a new invocation
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter([])
    third_invocation = add_single_inv(1, 3)
    assert third_invocation.invocation_id != next_invocation.invocation_id


def set_invocation_exception() -> None:
    raise NotImplementedError()
    # if failed in self.app.state_backend.set_exception(invocation, exception)
    # then the status did not change and the code gets waiting forever
    # check that it will
