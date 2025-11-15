from typing import TYPE_CHECKING

import pytest

from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.invocation import DistributedInvocation
    from pynenc.task import Task

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


@pytest.fixture
def task(app_instance: "Pynenc") -> "Task":
    """Helper to create a dummy invocation."""
    dummy_task.app = app_instance
    return dummy_task


def test_route_and_retrieve_invocation(task: "Task") -> None:
    """Test that invocations can be routed and retrieved in FIFO order."""
    broker = task.app.broker

    inv_1: DistributedInvocation = task()  # type: ignore
    assert broker.retrieve_invocation() == inv_1
    inv_2: DistributedInvocation = task()  # type: ignore
    assert broker.retrieve_invocation() == inv_2

    broker.route_invocation(inv_1)
    broker.route_invocation(inv_1)
    broker.route_invocation(inv_2)
    broker.route_invocation(inv_1)
    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() == inv_2
    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() is None


def test_route_and_retrieve_multiple_invocations(task: "Task") -> None:
    """Test that multiple invocations can be routed and retrieved in FIFO order."""
    broker = task.app.broker

    assert broker.retrieve_invocation() is None

    # It will route automatically when calling the task
    inv_1: DistributedInvocation = task()  # type: ignore
    inv_2: DistributedInvocation = task()  # type: ignore

    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() == inv_2
    assert broker.retrieve_invocation() is None

    # Route again with reoute_invocations
    broker.route_invocations([inv_1, inv_2, inv_1])
    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() == inv_2
    assert broker.retrieve_invocation() == inv_1
    assert broker.retrieve_invocation() is None


def test_count_invocations(task: "Task") -> None:
    """Test that counting invocations works correctly."""
    broker = task.app.broker

    assert broker.count_invocations() == 0

    inv_1: DistributedInvocation = task()  # type: ignore
    inv_2: DistributedInvocation = task()  # type: ignore

    assert broker.count_invocations() == 2
    assert broker.retrieve_invocation() == inv_1
    assert broker.count_invocations() == 1
    assert broker.retrieve_invocation() == inv_2
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None
    assert broker.count_invocations() == 0

    broker.route_invocations([inv_1, inv_2, inv_1])
    assert broker.count_invocations() == 3
    assert broker.retrieve_invocation() == inv_1
    assert broker.count_invocations() == 2


def test_purge_invocations(task: "Task") -> None:
    """Test that purging invocations works correctly."""
    broker = task.app.broker

    assert broker.count_invocations() == 0

    _ = task()
    _ = task()

    assert broker.count_invocations() == 2

    broker.purge()
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None

    # Purge again to test idempotency
    broker.purge()
    assert broker.count_invocations() == 0
    assert broker.retrieve_invocation() is None
