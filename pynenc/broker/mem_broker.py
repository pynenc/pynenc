from collections import deque
from typing import TYPE_CHECKING, Optional

from ..invocation import DistributedInvocation
from .base_broker import BaseBroker

if TYPE_CHECKING:
    from ..app import Pynenc


class MemBroker(BaseBroker):
    def __init__(self, app: "Pynenc") -> None:
        self._queue: deque = deque()
        super().__init__(app)

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        self._queue.append(invocation.to_json())

    def retrieve_invocation(self) -> Optional["DistributedInvocation"]:
        if self._queue and (inv := self._queue.pop()):
            return DistributedInvocation.from_json(self.app, inv)
        return None

    def purge(self) -> None:
        return self._queue.clear()
