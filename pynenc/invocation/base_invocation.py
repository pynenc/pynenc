from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, TYPE_CHECKING, Generic, TypeVar, ParamSpec
import uuid

if TYPE_CHECKING:
    from ..task import BaseTask

    P = ParamSpec("P")
else:
    P = TypeVar("P")
R = TypeVar("R")


class BaseInvocation(ABC, Generic[R]):
    """"""

    arguments: dict[str, Any]

    @property
    @abstractmethod
    def value(self) -> R:
        """"""


class SynchronousInvocation(BaseInvocation[R]):
    """The SynchronousInvocation class is used only for dev"""

    def __init__(self, value: R, arguments: dict[str, Any]) -> None:
        self._value = value
        self.arguments = arguments

    @property
    def value(self) -> R:
        return self._value


class DistributedInvocation(BaseInvocation[R]):
    """"""

    def __init__(self, task: BaseTask[P, R], arguments: dict[str, Any]) -> None:
        self.app = task.app
        self.task = task
        self.arguments = arguments
        self.invocation_id = str(uuid.uuid4())
        self.app.state_backend.insert_invocation(self)

    @property
    def value(self) -> R:
        ...
        # self.app.state_backend...
