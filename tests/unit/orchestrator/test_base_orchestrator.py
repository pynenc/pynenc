from pynenc.conf import ConcurrencyControlType
from pynenc.invocation import DistributedInvocation, InvocationStatus
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


@mock_base_app.task(registration_concurrency=ConcurrencyControlType.TASK)
def add_single_inv(x: int, y: int) -> int:
    return x + y


def test_registration_concurrency(mock_base_app: MockPynenc) -> None:
    """Test that the when `task.options.registration_concurrency` is set the orchestrator
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


@mock_base_app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def dummy_run_disable_concurrency() -> None:
    pass


def test_running_concurrency_disabled(mock_base_app: MockPynenc) -> None:
    """Test that when `task.options.running_concurrency` is disabled
    is_authorize_to_run_by_concurrency_control will always return True
    """
    running_invocation = dummy_run_disable_concurrency()
    to_run_invocation = dummy_run_disable_concurrency()
    assert isinstance(to_run_invocation, DistributedInvocation)
    # If there's no invocation it can run
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter([])
    assert mock_base_app.orchestrator.is_authorize_to_run_by_concurrency_control(
        to_run_invocation
    )
    # but also if there's one running invocation
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter(
        [running_invocation]
    )
    assert mock_base_app.orchestrator.is_authorize_to_run_by_concurrency_control(
        to_run_invocation
    )


@mock_base_app.task(running_concurrency=ConcurrencyControlType.TASK)
def dummy_run_task_concurrency() -> None:
    pass


def test_running_concurrency_task_control(mock_base_app: MockPynenc) -> None:
    """Test that when `task.options.running_concurrency` is set the orchestrator
    get_invocations_to_run will only return the invocation is there's no other running
    for the same task
    """
    # It will return False
    running_invocation = dummy_run_task_concurrency()
    to_run_invocation = dummy_run_task_concurrency()
    # If there's no invocation it can run
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter([])
    assert isinstance(to_run_invocation, DistributedInvocation)
    assert mock_base_app.orchestrator.is_authorize_to_run_by_concurrency_control(
        to_run_invocation
    )
    # But if exists a running invocation of the same task it will return False
    mock_base_app.orchestrator.get_existing_invocations.return_value = iter(
        [running_invocation]
    )
    assert not mock_base_app.orchestrator.is_authorize_to_run_by_concurrency_control(
        to_run_invocation
    )
