from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Any, Optional, TypeVar
import uuid

from ..types import Params, Result

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..arguments import Arguments
    from ..call import Call
    from ..task import Task


T = TypeVar("T", bound="BaseInvocation")


@dataclass(frozen=True)
class BaseInvocation(ABC, Generic[Params, Result]):
    """Invocation of a task call

    A call can have several invocations in the system"""

    call: "Call[Params, Result]"

    @property
    def app(self) -> "Pynenc":
        return self.call.app

    @property
    def task(self) -> "Task[Params, Result]":
        return self.call.task

    @property
    def arguments(self) -> "Arguments":
        return self.call.arguments

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
    def result(self) -> "Result":
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
