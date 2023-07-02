from __future__ import annotations
from functools import cached_property
from typing import TYPE_CHECKING

from .base_invocation import BaseInvocation
from ..types import Params, Result


if TYPE_CHECKING:
    from ..arguments import Arguments
    from ..task import Task


class SynchronousInvocation(BaseInvocation[Params, Result]):
    def __init__(
        self, task: "Task[Params, Result]", arguments: "Arguments", result: "Result"
    ) -> None:
        super().__init__(task, arguments)
        self._result = result

    @cached_property
    def result(self) -> "Result":
        return self._result
