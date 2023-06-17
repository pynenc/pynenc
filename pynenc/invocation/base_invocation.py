from __future__ import annotations
from abc import ABC, abstractmethod
from dataclasses import dataclass
from functools import cached_property
from typing import TYPE_CHECKING, Generic
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
