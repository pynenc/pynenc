from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

from ..invocation import InvocationStatus, DistributedInvocation
from ..types import Params, Result, Args

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task


class BaseBroker(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @abstractmethod
    def route_invocation(self, invocation: DistributedInvocation) -> None:
        ...

    def route_task(
        self, task: "Task[Params, Result]", arguments: "Args"
    ) -> DistributedInvocation[Result]:
        self.route_invocation(invocation := DistributedInvocation(task, arguments))
        self.app.orchestrator.set_invocation_status(
            invocation, InvocationStatus.REGISTERED
        )
        return invocation
