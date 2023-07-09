from __future__ import annotations
from functools import cached_property
from typing import TYPE_CHECKING

from .base_invocation import BaseInvocation
from ..types import Params, Result


if TYPE_CHECKING:
    from ..call import Call


class SynchronousInvocation(BaseInvocation[Params, Result]):
    def __init__(self, call: "Call[Params, Result]", result: "Result") -> None:
        super().__init__(call)
        self._result = result

    @cached_property
    def result(self) -> "Result":
        return self._result
