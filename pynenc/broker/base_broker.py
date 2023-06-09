from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional

from ..call import Call
from ..invocation import InvocationStatus, DistributedInvocation
from ..types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task


class BaseBroker(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @abstractmethod
    def _route_invocation(self, invocation: DistributedInvocation) -> None:
        ...

    @abstractmethod
    def _retrieve_invocation(self) -> Optional[DistributedInvocation]:
        ...

    # @abstractmethod
    # def _acknowledge_invocation(self, invocation: DistributedInvocation) -> None:
    #     ...

    # @abstractmethod
    # def _requeue_invocation(self, invocation: DistributedInvocation) -> None:
    #     ...

    def route_call(
        self, call: "Call[Params, Result]"
    ) -> DistributedInvocation[Params, Result]:
        """Creates a new invocation and routes it"""
        self.route_invocation(
            invocation := DistributedInvocation(
                call, parent_invocation=self.app.invocation_context
            )
        )
        return invocation

    def route_invocation(self, invocation: DistributedInvocation) -> None:
        """Routes the invocation and change status"""
        self._route_invocation(invocation)
        self.app.orchestrator.set_invocation_status(
            invocation, InvocationStatus.REGISTERED
        )

    def retrieve_invocation(self) -> Optional[DistributedInvocation]:
        """Returns an invocation if any and change status"""
        if invocation := self._retrieve_invocation():
            self.app.orchestrator.set_invocation_status(
                invocation, InvocationStatus.PENDING
            )
        return invocation
