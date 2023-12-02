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
    sleep(0.2)
    assert (
        app.orchestrator.get_invocation_status(dummy_invocation)
        == InvocationStatus.REGISTERED
    )
