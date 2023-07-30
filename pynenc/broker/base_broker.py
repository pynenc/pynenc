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
    def route_invocation(self, invocation: DistributedInvocation) -> None:
        ...

    @abstractmethod
    def retrieve_invocation(self) -> Optional[DistributedInvocation]:
        ...

    @abstractmethod
    def purge(self) -> None:
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
