from unittest.mock import MagicMock
from typing import TYPE_CHECKING
from functools import cached_property

import pytest

from pynenc import Pynenc
from pynenc.orchestrator.base_orchestrator import BaseOrchestrator
from pynenc.broker.base_broker import BaseBroker
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.conf import SingleInvocation


if TYPE_CHECKING:
    from pynenc.state_backend import MemStateBackend


class MockBroker(BaseBroker):
    route_invocation = MagicMock()


class MockBaseOrchestrator(BaseOrchestrator):
    get_existing_invocations = MagicMock()
    set_invocation_status = MagicMock()


class MockPynenc(Pynenc):
    @cached_property
    def broker(self) -> MockBroker:
        return MockBroker(self)

    @cached_property
    def orchestrator(self) -> MockBaseOrchestrator:
        return MockBaseOrchestrator(self)


@pytest.fixture
def app() -> MockPynenc:
    return MockPynenc()


def test_route_default(app: MockPynenc) -> None:
    """Test that the orchestrator will route the task by default

    If there are no options:
     - The orchestrator will forward the task to the broker
     - The broker should return a new Invocation and report the change of status to the orchestrator
    """

    @app.task
    def add(x: int, y: int) -> int:
        return x + y

    invocation = add(1, 3)
    assert isinstance(invocation, DistributedInvocation)
    # test that app.broker.route_invocation (MockBroker.route_invocation) has been called
    app.broker.route_invocation.assert_called_once()
    # test that app.orchestrator.set_invocation_status (MockBaseOrchestrator.set_invocation_status)
    # has been called with (result, InvocationStatus.REGISTERED)
    app.orchestrator.set_invocation_status.assert_called_once_with(
        invocation, InvocationStatus.REGISTERED
    )


def test_single_invocation(app: MockPynenc) -> None:
    """Test that the when `task.options.single_invocation` is set the orchestrator
    will only route the task if do not exists a Pending instance
    """

    @app.task(single_invocation=SingleInvocation())
    def add(x: int, y: int) -> int:
        return x + y

    # Get existing invocation doesn't find any pending match
    app.orchestrator.get_existing_invocations.return_value = iter([])
    first_invocation = add(1, 3)
    # Return previous as a pending match
    app.orchestrator.get_existing_invocations.return_value = iter([first_invocation])
    next_invocation = add(1, 3)
    assert first_invocation.invocation_id == next_invocation.invocation_id
    # Back to no match, generates a new invocation
    app.orchestrator.get_existing_invocations.return_value = iter([])
    third_invocation = add(1, 3)
    assert third_invocation.invocation_id != next_invocation.invocation_id
