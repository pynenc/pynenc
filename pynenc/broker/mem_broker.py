from collections import deque
from typing import TYPE_CHECKING, Optional

from pynenc.broker.base_broker import BaseBroker
from pynenc.invocation.dist_invocation import DistributedInvocation

if TYPE_CHECKING:
    from ..app import Pynenc


class MemBroker(BaseBroker):
    """
    An in-memory implementation of the BaseBroker.

    This subclass of BaseBroker implements the abstract methods for routing,
    retrieving, and purging invocations using an in-memory deque. It's primarily
    intended for testing and demonstration purposes.

    ```{warning}
    The `MemBroker` class generates the queue in the process's memory and is not suitable
    for production systems. Its use should be limited to testing or demonstration purposes only.
    ```

    :param Pynenc app: A reference to the Pynenc application.
    """

    def __init__(self, app: "Pynenc") -> None:
        self._queue: deque = deque()
        super().__init__(app)

    def route_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Route an invocation by adding it to the in-memory queue.

        This method serializes the DistributedInvocation object to JSON
        and appends it to the deque, effectively queuing it for processing.

        :param DistributedInvocation invocation: The invocation to be queued.
        """
        self._queue.append(invocation.to_json())

    def retrieve_invocation(self) -> Optional["DistributedInvocation"]:
        """
        Retrieve the next invocation from the queue.

        This method pops the next item from the deque, deserializes it from JSON,
        and returns the DistributedInvocation object. If the queue is empty,
        it returns None.

        :return:
            The next invocation from the queue, or None if the queue is empty.
        """
        if self._queue and (inv := self._queue.pop()):
            return DistributedInvocation.from_json(self.app, inv)
        return None

    def purge(self) -> None:
        """
        Clear all invocations from the in-memory queue.

        This method empties the deque, removing all pending invocations.
        """
        return self._queue.clear()
