from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.conf.config_broker import ConfigBroker

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

    @cached_property
    def conf(self) -> ConfigBroker:
        return ConfigBroker(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @abstractmethod
    def route_invocation(self, invocation_id: "InvocationId") -> None:
        """
        Abstract method for routing a given invocation id.

        This method should define the process of handling and dispatching a given
        invocation id within the broker system. Implementations might involve
        sending the invocation to a queue or handling it internally.

        :param InvocationId invocation_id: The invocation id to be routed.
        """

    @abstractmethod
    def route_invocations(self, invocation_ids: list["InvocationId"]) -> None:
        """
        Routes multiple invocations at once.

        This method is used for batch processing of invocations to improve performance
        when parallelizing large numbers of tasks.

        Default implementation sequentially routes each invocation. Subclasses can
        override this with more efficient batch processing implementations.

        :param list["InvocationId"] invocation_ids: The invocation ids to be routed.
        """

    @abstractmethod
    def retrieve_invocation(self) -> "InvocationId | None":
        """
        Method to retrieve a distributed invocation id.

        Implementations of this method should detail how to retrieve the next
        available invocation from the broker's queue or storage system. It is
        expected to return a invocation id if one is available, or None
        if the queue is empty.

        :return: The next invocation id to be processed, or None.
        """

    @abstractmethod
    def count_invocations(self) -> int:
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
