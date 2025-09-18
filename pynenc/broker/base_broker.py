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
    Abstract base class for message brokers in Pynenc's plugin system.

    This class serves as the foundational interface for implementing various message brokers
    across different backend systems. The plugin architecture allows for flexible integration
    with multiple storage and messaging solutions, each providing their own broker implementation.

    The broker is responsible for routing task invocations through a message queue system,
    supporting both FIFO and priority-based queuing strategies depending on the implementation.

    :param Pynenc app: A reference to the Pynenc application instance.

    ```{note}
    Available broker implementations depend on installed plugins:
    - **MemBroker**: Built-in memory-based broker for development and testing
    - **RedisBroker**: Available with `pynenc-redis` plugin for production Redis deployments
    - **MongoBroker**: Available with `pynenc-mongodb` plugin for MongoDB-based systems
    ```

    ```{attention}
    Plugin-specific brokers may have different capabilities and performance characteristics.
    Consult the documentation for your chosen backend plugin for implementation-specific details.
    ```

    ```{hint}
    The plugin system allows for easy extension with custom broker implementations.
    Create a custom broker by subclassing BaseBroker and implementing the abstract methods.
    ```

    ```{seealso}
    For production deployments, consider Redis or MongoDB plugins which provide
    persistent, distributed message queuing capabilities.
    ```

    ### Examples
    ```{code-block} python
        # The broker is automatically selected based on your plugin configuration
        # No direct instantiation needed - handled by Pynenc application
        invocation = app.broker.route_call(call)
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
    def route_invocations(self, invocations: list[DistributedInvocation]) -> None:
        """
        Routes multiple invocations at once.

        This method is used for batch processing of invocations to improve performance
        when parallelizing large numbers of tasks.

        Default implementation sequentially routes each invocation. Subclasses can
        override this with more efficient batch processing implementations.

        :param list[DistributedInvocation] invocations: The invocations to be routed.
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
