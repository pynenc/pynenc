from time import sleep
from typing import TYPE_CHECKING

from pynenc.invocation import DistributedInvocation, InvocationStatus

if TYPE_CHECKING:
    pass


def test_pending_status_expiration(dummy_invocation: "DistributedInvocation") -> None:
    app = dummy_invocation.app
    app.orchestrator.register_new_invocations([dummy_invocation])
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation.invocation_id)
        == InvocationStatus.REGISTERED
    )

    app.conf.max_pending_seconds = 0.1
    app.orchestrator.set_invocation_status(
        dummy_invocation.invocation_id, InvocationStatus.PENDING
    )
    # if we check immediately it should return PENDING
    assert app.orchestrator.get_invocation_status(dummy_invocation.invocation_id) in [
        InvocationStatus.PENDING,
    ]
    # if we wait enough to expire the pending seconds
    sleep(app.conf.max_pending_seconds * 2)
    # It should expire the PENDING and fallback to previous status REGISTERED
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation.invocation_id)
        == InvocationStatus.REGISTERED
    )
