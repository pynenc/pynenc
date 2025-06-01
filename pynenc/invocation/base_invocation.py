from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from collections.abc import AsyncGenerator, Iterator
from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Any, Generic, TypeVar

from pynenc.call import Call
from pynenc.types import Params, Result
from pynenc.util.log import TaskLoggerAdapter
from pynenc.workflow.identity import WorkflowIdentity

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..arguments import Arguments
    from ..task import Task
    from .status import InvocationStatus


T = TypeVar("T", bound="BaseInvocation")


@dataclass(frozen=True)
class InvocationIdentity(Generic[Params, Result]):
    """Immutable identity of an invocation"""

    call: Call[Params, Result]
    invocation_id: str = field(default_factory=lambda: str(uuid.uuid4()))
    parent_invocation: BaseInvocation | None = None


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
    This distinction is crucial for orchestration and cycle control within the system.

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
        parent_invocation: BaseInvocation | None = None,
        invocation_id: str | None = None,
        workflow: WorkflowIdentity | None = None,
    ):
        """Initialize the invocation with its identity."""
        self.identity = InvocationIdentity(
            call=call,
            invocation_id=invocation_id or str(uuid.uuid4()),
            parent_invocation=parent_invocation if parent_invocation else None,
        )
        self.workflow: WorkflowIdentity = workflow or WorkflowIdentity.from_invocation(
            self.identity
        )
        self.init_logger()

    def init_logger(self) -> None:
        """Initialize the logger for the invocation."""
        self.task.logger = TaskLoggerAdapter(
            self.app.logger, self.task.task_id, self.invocation_id
        )

    @property
    def call(self) -> Call[Params, Result]:
        return self.identity.call

    @property
    def invocation_id(self) -> str:
        return self.identity.invocation_id

    @property
    def parent_invocation(self) -> BaseInvocation | None:
        return self.identity.parent_invocation

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
    def serialized_arguments(self) -> dict[str, str]:
        return self.call.serialized_arguments

    @abstractmethod
    def to_json(self) -> str:
        """:return: The serialized invocation"""

    @classmethod
    @abstractmethod
    def from_json(cls: type[T], app: Pynenc, serialized: str) -> T:
        """:return: a new invocation from a serialized invocation"""

    @property
    def call_id(self) -> str:
        return self.call.call_id

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
        return self.identity == other.identity and self.status == other.status


@dataclass(frozen=True)
class BaseInvocationGroup(ABC, Generic[Params, Result, T]):
    """
    Abstract base class for grouping multiple invocations of a specific task.

    This class is designed to aggregate a collection of invocations, each represented by a `BaseInvocation` or its subclasses. It is useful in scenarios where multiple invocations of a task need to be managed or processed together.

    Subclasses of `BaseInvocationGroup`, such as `ConcurrentInvocationGroup` and `DistributedInvocationGroup`, provide specific implementations for synchronous and distributed environments, respectively.

    :param Task task: The task associated with the invocations.
    :param list[BaseInvocation] invocations: A list of invocations, each an instance of a `BaseInvocation` subclass.
    """

    task: Task
    invocations: list[T]

    @property
    def app(self) -> Pynenc:
        return self.task.app

    def __iter__(self) -> Iterator[T]:
        yield from self.invocations

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
