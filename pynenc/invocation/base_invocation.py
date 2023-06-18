from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Generic, Any
import uuid

from ..types import Params, Result, Args

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..task import Task


@dataclass(frozen=True)
class BaseInvocation(ABC, Generic[Params, Result]):
    """"""

    task: "Task[Params, Result]"
    arguments: "Args"

    @cached_property
    def app(self) -> "Pynenc":
        return self.task.app

    @cached_property
    def invocation_id(self) -> str:
        return str(uuid.uuid4())

    @property
    @abstractmethod
    def result(self) -> "Result":
        """"""

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(invocation_id={self.invocation_id}, task={self.task}, arguments={self.arguments})"

    def __repr__(self) -> str:
        return self.__str__()

    def __hash__(self) -> int:
        return hash((self.task, self.arguments, self.invocation_id))

    def __eq__(self, other: Any) -> bool:
        if not isinstance(other, BaseInvocation):
            return False
        return (self.task, self.arguments, self.invocation_id) == (
            other.task,
            other.arguments,
            other.invocation_id,
        )
