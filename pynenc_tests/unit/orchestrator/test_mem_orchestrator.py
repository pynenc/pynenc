import threading

import pytest

from pynenc.exceptions import PendingInvocationLockError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from pynenc.orchestrator.mem_orchestrator import MemOrchestrator
from pynenc_tests.conftest import Pynenc

app = Pynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_set_pending_status_concurrent_access() -> None:
    invocation: DistributedInvocation = add(1, 2)  # type: ignore

    orc = app.orchestrator
    assert isinstance(orc, MemOrchestrator)

    def task() -> None:
        with pytest.raises(PendingInvocationLockError):
            app.orchestrator.set_invocation_pending_status(invocation.invocation_id)

    # Simulate concurrent access
    orc.locks[invocation.invocation_id] = threading.Lock()
    orc.locks[invocation.invocation_id].acquire()

    thread = threading.Thread(target=task)
    thread.start()
    thread.join()

    # Release the lock for cleanup
    orc.locks[invocation.invocation_id].release()


def test_set_pending_status_already_pending() -> None:
    orc = app.orchestrator
    assert isinstance(orc, MemOrchestrator)

    invocation: DistributedInvocation = add(1, 2)  # type: ignore
    orc.invocation_status[invocation.invocation_id] = InvocationStatus.PENDING

    # Action & Assertion
    with pytest.raises(PendingInvocationLockError):
        orc.set_invocation_pending_status(invocation.invocation_id)
