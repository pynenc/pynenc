from abc import ABC, abstractmethod
from collections.abc import Sequence
from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.conf.config_broker import (
    ConfigBroker,
    validate_priority,
    validate_queue_name,
)
from pynenc.exceptions import ConfigError

if TYPE_CHECKING:
    from ..app import Pynenc
    from pynenc.identifiers.invocation_id import InvocationId


class BaseBroker(ABC):
    """
    Abstract base class for message brokers in Pynenc's plugin system.

    Routes task invocations through a message queue system, supporting both FIFO
    and priority-based queuing depending on the implementation.

    :param Pynenc app: A reference to the Pynenc application instance.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self._warned_queue_mismatches: set[tuple[str, ...]] = set()
        self._configured_queue_set_cache: (
            tuple[tuple[str, ...], frozenset[str]] | None
        ) = None

    @cached_property
    def conf(self) -> ConfigBroker:
        return ConfigBroker(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def route_invocation(
        self, invocation_id: "InvocationId", queue_name: str, priority: float
    ) -> None:
        """
        Route an invocation ID to a broker queue with a routing priority.

        The broker only queues IDs. Callers resolve task configuration before
        calling this method.

        :param InvocationId invocation_id: The ID of the invocation to queue.
        :param str queue_name: The queue where the invocation should be routed.
        :param float priority: The queue priority for this invocation.
        """
        self._validate_queue_names((queue_name,))
        validate_priority(priority, label="Broker priority")
        self._route_invocation(invocation_id, queue_name, priority)

    def route_invocations(
        self,
        invocation_ids: Sequence["InvocationId"],
        queue_name: str,
        priority: float,
    ) -> None:
        """
        Route multiple invocation IDs to one broker queue with one priority.

        This method is used for batch processing of invocations to improve
        performance when parallelizing large numbers of tasks.

        :param Sequence[InvocationId] invocation_ids:
            The invocation IDs to be queued.
        :param str queue_name: The queue where the invocations should be routed.
        :param float priority: The queue priority for these invocations.
        """
        self._validate_queue_names((queue_name,))
        validate_priority(priority, label="Broker priority")
        self._route_invocations(invocation_ids, queue_name, priority)

    @abstractmethod
    def _route_invocation(
        self, invocation_id: "InvocationId", queue_name: str, priority: float
    ) -> None:
        """
        Queue an invocation ID after base broker validation.

        :param InvocationId invocation_id: The ID of the invocation to queue.
        :param str queue_name: The queue where the invocation should be routed.
        :param float priority: The queue priority for this invocation.
        """

    @abstractmethod
    def _route_invocations(
        self,
        invocation_ids: Sequence["InvocationId"],
        queue_name: str,
        priority: float,
    ) -> None:
        """
        Queue multiple invocation IDs after base broker validation.

        Implementations may optimize this batch path or simply loop over
        ``_route_invocation``.

        :param Sequence[InvocationId] invocation_ids:
            The invocation IDs to queue.
        :param str queue_name: The queue where the invocations should be routed.
        :param float priority: The queue priority for these invocations.
        """

    def _validate_queue_names(self, queue_names: Sequence[str]) -> None:
        """Validate queue names and apply the broker mismatch policy."""
        for queue_name in queue_names:
            validate_queue_name(queue_name)
        configured_queues = self.conf.queues
        configured_queue_set = self.configured_queue_set
        unknown_queues = tuple(
            dict.fromkeys(
                queue_name
                for queue_name in queue_names
                if queue_name not in configured_queue_set
            )
        )
        if not unknown_queues:
            return
        message = (
            "Broker uses queue(s) not configured in broker.queues: "
            f"{', '.join(unknown_queues)}. Configured queues: {configured_queues!r}. "
            "This is allowed for flexible deployments."
        )
        if self.conf.raise_on_queue_mismatch:
            raise ConfigError(message)
        if not self.conf.warn_on_queue_mismatch:
            return
        if unknown_queues in self._warned_queue_mismatches:
            return
        self._warned_queue_mismatches.add(unknown_queues)
        self.app.logger.warning(message)

    @property
    def configured_queue_set(self) -> frozenset[str]:
        """Return configured broker queues as a set for mismatch checks."""
        configured_queues = self.conf.queues
        if (
            self._configured_queue_set_cache is None
            or self._configured_queue_set_cache[0] != configured_queues
        ):
            self._configured_queue_set_cache = (
                configured_queues,
                frozenset(configured_queues),
            )
        return self._configured_queue_set_cache[1]

    @abstractmethod
    def retrieve_invocation(
        self, queue_name: str | None = None
    ) -> "InvocationId | None":
        """
        Method to retrieve a distributed invocation id.

        Implementations of this method should detail how to retrieve the next
        available invocation from the broker's queue or storage system. It is
        expected to return a invocation id if one is available, or None
        if the queue is empty.

        :return: The next invocation id to be processed, or None.
        """

    @abstractmethod
    def count_invocations(self, queue_names: Sequence[str] | None = None) -> int:
        """
        Method to count the number of invocations in the queue.

        This method should return the current count of pending invocations in the
        broker's queue. It's useful for monitoring and managing the queue's state.

        :return: The number of invocations in the queue.
        """

    @abstractmethod
    def purge(self) -> None:
        """
        Method to purge the message queue.

        This method is intended to clear or reset the state of the broker's queue,
        removing all pending invocations. It's crucial for error handling and
        managing the queue in specific situations like maintenance or reset.
        """
