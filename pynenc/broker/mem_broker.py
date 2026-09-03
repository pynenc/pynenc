from collections import defaultdict, deque
from collections.abc import Sequence
from typing import TYPE_CHECKING

from pynenc.broker.base_broker import BaseBroker

if TYPE_CHECKING:
    from ..app import Pynenc
    from pynenc.identifiers.invocation_id import InvocationId


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
        self._queues: dict[str, dict[float, deque[InvocationId]]] = defaultdict(
            lambda: defaultdict(deque)
        )
        super().__init__(app)

    def _route_invocation(
        self, invocation_id: "InvocationId", queue_name: str, priority: float
    ) -> None:
        """
        Route an invocation id by adding it to the in-memory queue.

        This method appends the invocation ID to the deque, effectively queuing it for processing.

        :param InvocationId invocation_id: The ID of the invocation to be queued.
        :param str queue_name: The queue where the invocation should be routed.
        :param float priority: The queue priority for this invocation.
        """
        self._queues[queue_name][priority].append(invocation_id)

    def _route_invocations(
        self,
        invocation_ids: Sequence["InvocationId"],
        queue_name: str,
        priority: float,
    ) -> None:
        """Route multiple invocation IDs in memory."""
        for invocation_id in invocation_ids:
            self._route_invocation(invocation_id, queue_name, priority)

    def retrieve_invocation(
        self, queue_name: str | None = None
    ) -> "InvocationId | None":
        """
        Retrieve the next invocation id from the queue.

        This method pops the next item from the deque and returns the invocation ID.
        If the queue is empty, it returns None.

        :return: The next invocation id from the queue, or None if the queue is empty.
        :rtype: InvocationId | None
        """
        queue = self.conf.queues[0] if queue_name is None else queue_name
        self._validate_queue_names((queue,))
        for priority in sorted(self._queues[queue], reverse=True):
            if self._queues[queue][priority]:
                return self._queues[queue][priority].popleft()
        return None

    def count_invocations(self, queue_names: Sequence[str] | None = None) -> int:
        """
        Get the number of invocations in the in-memory queue.
        """
        queues = self.conf.queues if queue_names is None else queue_names
        self._validate_queue_names(queues)
        return sum(
            len(invocations)
            for queue_name in queues
            for invocations in self._queues[queue_name].values()
        )

    def purge(self) -> None:
        """
        Clear all invocations from the in-memory queue.

        This method empties the deque, removing all pending invocations.
        """
        self._queues.clear()
