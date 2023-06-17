from __future__ import annotations
from typing import Any, TYPE_CHECKING, TypeVar, ParamSpec
import uuid

from .base_invocation import BaseInvocation

if TYPE_CHECKING:
    from ..task import BaseTask

    P = ParamSpec("P")
else:
    P = TypeVar("P")
R = TypeVar("R")


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
