from time import sleep
from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation, InvocationStatus

if TYPE_CHECKING:
    pass


def test_pending_status_expiration(dummy_invocation: "DistributedInvocation") -> None:
    app = dummy_invocation.app
    app.orchestrator.set_invocation_status(
        dummy_invocation, InvocationStatus.REGISTERED
    )
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation)
        == InvocationStatus.REGISTERED
    )

    app.conf.max_pending_seconds = 0.1
    app.orchestrator.set_invocation_status(dummy_invocation, InvocationStatus.PENDING)
    sleep(0.2)  # enough to expire the pending seconds
    # ! Why checking twice?
    # * The first check may still return PENDING for the redis orchestrator
    #   That is because the process to check pending timeout is async
    #   It will return existing status immediately, without waiting for pending validation
    assert app.orchestrator.get_invocation_status(dummy_invocation) in [
        InvocationStatus.PENDING,
        InvocationStatus.REGISTERED,
    ]
    # * The second check will return the correct status
    #   As the async thread had time to set the expired status
    sleep(0.2)  # pending status process is async
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation)
        == InvocationStatus.REGISTERED
    )
