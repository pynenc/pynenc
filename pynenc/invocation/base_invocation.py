from __future__ import annotations

from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pynenc.call import Call
from pynenc.identifiers.invocation_id import InvocationId, generate_invocation_id
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.arguments import Arguments
    from pynenc.task import Task
    from pynenc.invocation.status import InvocationStatus
    from pynenc.workflow.workflow_identity import WorkflowIdentity


T = TypeVar("T", bound="BaseInvocation")


class BaseInvocation(ABC, Generic[Params, Result]):
    """
    Base class for representing an invocation of a task call in a distributed system.

    In the context of the system, the following concepts are key:
    - Function: A standard Python function.
    - Task: A Pynenc object encapsulating a function, enabling it to run in a distributed environment.
      Tasks are unique by module and function name and cannot be nested.
    - Call: A specific call to a task with a set of arguments, unique per function and argument set.
    - **Invocation**: A specific execution instance of a call.

    A single task can be called with different arguments, and each call can be executed multiple times.

    The `BaseInvocation` class serves as a template for two key types of invocations:
    - `DistributedInvocation`: The primary invocation type used in the system for distributed execution.
    - `ConcurrentInvocation`: Used for local execution, primarily in testing environments without a runner.

    ```{important}
    Sync invocations cannot be used in production environments, only for testing in sync mode.
    ```

    :param Call[Params, Result] call: The specific call instance that this invocation represents.
    """

    def __init__(
        self,
        call: Call[Params, Result],
        invocation_id: InvocationId | None = None,
    ):
        """Initialize the invocation with its identity."""
        self._call = call
        self._invocation_id = invocation_id or generate_invocation_id()

    @property
    def call(self) -> Call[Params, Result]:
        """Get the call associated with this invocation."""
        return self._call

    @property
    def invocation_id(self) -> InvocationId:
        return self._invocation_id

    @property
    def app(self) -> Pynenc:
        return self.call.app

    @property
    def task(self) -> Task[Params, Result]:
        return self.call.task

    @property
    def arguments(self) -> Arguments:
        return self.call.arguments

    @property
    @abstractmethod
    def workflow(self) -> WorkflowIdentity:
        """"""

    @property
    @abstractmethod
    def status(self) -> InvocationStatus:
        """"""

    @property
    @abstractmethod
    def result(self) -> Result:
        """"""

    @abstractmethod
    async def async_result(self) -> Result:
        """"""

    @property
    @abstractmethod
    def num_retries(self) -> int:
        """"""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(invocation_id={self.invocation_id}, {self.call})"

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash(self.invocation_id)

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, BaseInvocation):
            return False
        return (
            self.invocation_id == other.invocation_id
            and self.call.call_id == other.call.call_id
        )


@dataclass(frozen=True)
class BaseInvocationGroup(ABC, Generic[Params, Result, T]):
    """
    Abstract base class for grouping multiple invocations of a specific task.

    This class is designed to aggregate a collection of invocations, each represented by a `BaseInvocation` or its subclasses. It is useful in scenarios where multiple invocations of a task need to be managed or processed together.

    Subclasses of `BaseInvocationGroup`, such as `ConcurrentInvocationGroup` and `DistributedInvocationGroup`, provide specific implementations for synchronous and distributed environments, respectively.

    :param Task task: The task associated with the invocations.
    :param dict[InvocationId, BaseInvocation] invocations: A dictionary of invocations, each an instance of a `BaseInvocation` subclass.
    """

    task: Task
    invocations: list[T]

    @cached_property
    def invocation_map(self) -> dict[InvocationId, T]:
        return {inv.invocation_id: inv for inv in self.invocations}

    @property
    def app(self) -> Pynenc:
        return self.task.app

    def __iter__(self) -> Iterator[T]:
        return iter(self.invocations)

    @property
    @abstractmethod
    def results(self) -> Iterator[Result]:
        """
        Provide an iterator over the results.

        :return Iterator[Result]: An iterator over the results of the invocations.
        """

    @abstractmethod
    def async_results(self) -> AsyncGenerator[Result, None]:
        """
        An async iterator over the results of the invocations in the group.

        This method asynchronously iterates over the `ConcurrentInvocation` instances,
        yielding the result of each invocation using their async_result method.

        :return: An async iterator over the results of each invocation in the group.
        :rtype: AsyncIterator[Result]
        """
