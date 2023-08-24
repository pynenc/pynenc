from dataclasses import dataclass
from time import sleep
from typing import TYPE_CHECKING

import pytest

from pynenc.arguments import Arguments
from pynenc.call import Call
from pynenc.exceptions import CycleDetectedError
from pynenc.invocation import DistributedInvocation, InvocationStatus
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from pynenc import Pynenc
    from pynenc.task import Task


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
