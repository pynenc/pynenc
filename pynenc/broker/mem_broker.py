from collections import deque
from typing import TYPE_CHECKING

from pynenc.broker.base_broker import BaseBroker

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

    def route_invocation(self, invocation_id: str) -> None:
        """
        Route an invocation id by adding it to the in-memory queue.

        This method appends the invocation ID to the deque, effectively queuing it for processing.

        :param str invocation_id: The ID of the invocation to be queued.
        """
        self._queue.append(invocation_id)

    def route_invocations(self, invocation_ids: list[str]) -> None:
        """
        Routes multiple invocation IDs at once.

        :param list[str] invocation_ids: The invocation IDs to be routed.
        """
        for invocation_id in invocation_ids:
            self.route_invocation(invocation_id)

        if invocation_ids:
            self.app.logger.debug(f"Batch routed {len(invocation_ids)} invocations")

    def retrieve_invocation(self) -> str | None:
        """
        Retrieve the next invocation id from the queue.

        This method pops the next item from the deque and returns the invocation ID.
        If the queue is empty, it returns None.

        :return:
            The next invocation id from the queue, or None if the queue is empty.
        """
        if self._queue:
            return self._queue.popleft()
        return None

    def count_invocations(self) -> int:
        """
        Get the number of invocations in the in-memory queue.
        """
        return len(self._queue)

    def purge(self) -> None:
        """
        Clear all invocations from the in-memory queue.

        This method empties the deque, removing all pending invocations.
        """
        return self._queue.clear()
