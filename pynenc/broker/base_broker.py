from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Optional

from ..call import Call
from ..conf.config_broker import ConfigBroker
from ..context import dist_inv_context
from ..invocation import DistributedInvocation
from ..types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc


class BaseBroker(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @cached_property
    def conf(self) -> ConfigBroker:
        return ConfigBroker(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

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
                call, parent_invocation=dist_inv_context.get(self.app.app_id)
            )
        )
        self.app.logger.debug(
            f"Routed {call=} on invocation {invocation.invocation_id}"
        )
        return invocation
