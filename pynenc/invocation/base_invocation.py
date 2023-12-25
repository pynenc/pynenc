from __future__ import annotations

import uuid
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Iterator, TypeVar

from ..call import Call
from ..types import Params, Result
from ..util.log import TaskLoggerAdapter

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..arguments import Arguments
    from ..task import Task
    from .status import InvocationStatus


T = TypeVar("T", bound="BaseInvocation")


@dataclass(frozen=True)
class BaseInvocation(ABC, Generic[Params, Result]):
    """Invocation of a task call

    A call can have several invocations in the system"""

    call: Call[Params, Result]

    def __post_init__(self) -> None:
        self.task.logger = TaskLoggerAdapter(
            self.app.logger, self.task.task_id, self.invocation_id
        )

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
        """Returns a string with the serialized invocation"""

    @classmethod
    @abstractmethod
    def from_json(cls: type[T], app: Pynenc, serialized: str) -> T:
        """Returns a new invocation from a serialized invocation"""

    @property
    def call_id(self) -> str:
        return self.call.call_id

    @cached_property
    def invocation_id(self) -> str:
        """Returns a unique id for this invocation

        A task with the same arguments can have multiple invocations, the invocation id is used to differentiate them
        """
        return str(uuid.uuid4())

    @property
    @abstractmethod
    def status(self) -> InvocationStatus:
        """"""

    @property
    @abstractmethod
    def result(self) -> Result:
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
        # TODO equality based in task and arguments or in invocation_id?
        if not isinstance(other, BaseInvocation):
            return False
        return self.invocation_id == other.invocation_id


@dataclass(frozen=True)
class BaseInvocationGroup(ABC, Generic[Params, Result, T]):
    task: Task
    invocations: list[T]

    @property
    def app(self) -> Pynenc:
        return self.task.app

    @property
    def __iter__(self) -> Iterator[T]:
        yield from self.invocations

    @property
    @abstractmethod
    def results(self) -> Iterator[Result]:
        """"""
