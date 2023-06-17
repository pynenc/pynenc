from __future__ import annotations
from typing import TYPE_CHECKING
import uuid

from .base_invocation import BaseInvocation
from ..types import Params, Result, Args

if TYPE_CHECKING:
    from ..task import Task


class DistributedInvocation(BaseInvocation[Result]):
    """"""

    def __init__(self, task: "Task[Params, Result]", arguments: Args) -> None:
        self.app = task.app
        self.task = task
        self.arguments = arguments
        self.invocation_id = str(uuid.uuid4())
        self.app.state_backend.insert_invocation(self)

    @property
    def value(self) -> Result:
        raise NotImplementedError("TODO")
        # self.app.state_backend...
        # HERE SPECIFIC WAITING BEHAVIOUR!!!
