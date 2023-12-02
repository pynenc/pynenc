from collections import deque
from typing import TYPE_CHECKING, Optional

from .base_broker import BaseBroker

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation


class MemBroker(BaseBroker):
    def __init__(self, app: "Pynenc") -> None:
        self._queue: deque = deque()
        super().__init__(app)

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        self._queue.append(invocation)

    def retrieve_invocation(self) -> Optional["DistributedInvocation"]:
        return self._queue.pop() if self._queue else None

    def purge(self) -> None:
        return self._queue.clear()
