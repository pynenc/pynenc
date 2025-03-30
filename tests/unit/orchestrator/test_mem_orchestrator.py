import threading

import pytest

from pynenc.exceptions import PendingInvocationLockError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.orchestrator.mem_orchestrator import ArgPair, TaskInvocationCache
from tests.conftest import Pynenc

app = Pynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_argpair_initialization() -> None:
    key = "test_key"
    value = [1, 2, 3]
    pair = ArgPair(key, value)

    assert pair.key == key
    assert pair.value == value


def test_argpair_hash() -> None:
    pair1 = ArgPair("key", [1, 2, 3])
    pair2 = ArgPair("key", [1, 2, 3])
    pair3 = ArgPair("key", [4, 5, 6])

    assert hash(pair1) == hash(pair2)
    assert hash(pair1) != hash(pair3)


def test_argpair_equality() -> None:
    pair1 = ArgPair("key", [1, 2, 3])
    pair2 = ArgPair("key", [1, 2, 3])
    pair3 = ArgPair("key", [4, 5, 6])
    non_pair = "not_an_argpair"

    assert pair1 == pair2
    assert pair1 != pair3
    assert pair1 != non_pair


def test_argpair_string_representation() -> None:
    pair = ArgPair("key", [1, 2, 3])
    expected_str = "key:[1, 2, 3]"
    expected_repr = f"ArgPair({expected_str})"

    assert str(pair) == expected_str
    assert repr(pair) == expected_repr


def test_set_pending_status() -> None:
    # Setup
    cache: TaskInvocationCache = TaskInvocationCache(app)
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    # Set pending status can only be called on an invocation with some other status
    cache.set_status(invocation, InvocationStatus.RUNNING)

    # Test
    cache.set_pending_status(invocation)

    # Assertions
    assert cache.invocation_status[invocation.invocation_id] == InvocationStatus.PENDING
    assert cache.pending_timer[invocation.invocation_id] is not None


def test_set_pending_status_concurrent_access() -> None:
    cache: TaskInvocationCache = TaskInvocationCache(app)
    invocation: DistributedInvocation = add(1, 2)  # type: ignore

    def task() -> None:
        with pytest.raises(PendingInvocationLockError):
            cache.set_pending_status(invocation)

    # Simulate concurrent access
    cache.locks[invocation.invocation_id] = threading.Lock()
    cache.locks[invocation.invocation_id].acquire()

    thread = threading.Thread(target=task)
    thread.start()
    thread.join()

    # Release the lock for cleanup
    cache.locks[invocation.invocation_id].release()


def test_set_pending_status_already_pending() -> None:
    cache: TaskInvocationCache = TaskInvocationCache(app)
    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    cache.invocation_status[invocation.invocation_id] = InvocationStatus.PENDING

    # Action & Assertion
    with pytest.raises(PendingInvocationLockError):
        cache.set_pending_status(invocation)


def test_get_invocation_by_id() -> None:
    # Create an invocation
    invocation = add(1, 5)  # type: ignore
    invocation_id = invocation.invocation_id

    # Test getting the invocation by ID
    retrieved_invocation = app.orchestrator.get_invocation(invocation_id)
    assert retrieved_invocation is not None
    assert retrieved_invocation.invocation_id == invocation_id
    assert (
        retrieved_invocation.call.serialized_arguments
        == invocation.call.serialized_arguments
    )

    # Test with non-existent ID
    assert app.orchestrator.get_invocation("non-existent-id") is None
