from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Optional

from pynenc import context
from pynenc.call import Call
from pynenc.conf.config_broker import ConfigBroker
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc


class BaseBroker(ABC):
    """
    An Abstract Base Class for Message Brokers in Pynenc**

    This class serves as the foundational structure for implementing various message brokers.
    It currently supports a simple FIFO queue and is extendable for more complex functionalities
    like priority queues and integration with different databases and message queues.

    :param Pynenc app: A reference to the Pynenc application.

    ```{note}
    The `BaseBroker` is currently implemented with an in-memory queue (`MemBroker`) for testing and demonstration,
    and a Redis-backed queue (`RedisBroker`) for production use.
    ```

    ```{attention}
    The implementation is currently limited to a FIFO queue. Future enhancements will include support
    for priority queues and compatibility with other databases and message queues like RabbitMQ.
    ```

    ```{hint}
    The class is designed to be flexible and expandable, allowing for easy integration of additional
    features and message brokers in the future.
    ```

    ```{seealso}
    For more advanced or production-ready features, refer to the specific implementations like `RedisBroker`.
    ```

    The `route_call` method creates a new invocation and routes it, demonstrating a basic usage of the broker.

    ### Examples
    ```{code-block} python
        # Assuming `app` is an instance of Pynenc and `call` is a valid Call object
        broker = BaseBroker(app)
        invocation = broker.route_call(call)
    ```
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
    def route_invocation(self, invocation: DistributedInvocation) -> None:
        """
        Abstract method for routing a given invocation.

        This method should define the process of handling and dispatching a given
        DistributedInvocation within the broker system. Implementations might involve
        sending the invocation to a queue or handling it internally.

        :param DistributedInvocation invocation: The invocation to be routed.
        """

    @abstractmethod
    def retrieve_invocation(self) -> Optional[DistributedInvocation]:
        """
        Method to retrieve a distributed invocation.

        Implementations of this method should detail how to retrieve the next
        available invocation from the broker's queue or storage system. It is
        expected to return a DistributedInvocation if one is available, or None
        if the queue is empty.

        :return: The next invocation to be processed, or None.
        """

    @abstractmethod
    def purge(self) -> None:
        """
        Method to purge the message queue.

        This method is intended to clear or reset the state of the broker's queue,
        removing all pending invocations. It's crucial for error handling and
        managing the queue in specific situations like maintenance or reset.
        """

    # @abstractmethod
    # def _acknowledge_invocation(self, invocation: DistributedInvocation) -> None:
    #     ...

    # @abstractmethod
    # def _requeue_invocation(self, invocation: DistributedInvocation) -> None:
    #     ...

    def route_call(
        self, call: "Call[Params, Result]"
    ) -> DistributedInvocation[Params, Result]:
        """
        Creates and routes a new DistributedInvocation based on the given call.

        This method instantiates a DistributedInvocation with the provided call
        and the current invocation context. It then routes this invocation using
        the `route_invocation` method. This demonstrates the basic use of the
        broker's functionality.

        :param Call[Params, Result] call: The call object to be transformed into an invocation.

        :return: The routed invocation.

        ```{note}
        The method also logs the routing process for debugging purposes.
        ```
        """
        parent_invocation = context.get_dist_invocation_context(self.app.app_id)
        self.route_invocation(
            invocation := DistributedInvocation(
                call, parent_invocation=parent_invocation
            )
        )
        self.app.logger.debug(
            f"Routed {call=} on invocation {invocation.invocation_id}"
        )
        return invocation
