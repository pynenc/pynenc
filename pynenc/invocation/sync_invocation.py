from __future__ import annotations
from functools import cached_property
from typing import TYPE_CHECKING

from .base_invocation import BaseInvocation
from ..types import Params, Result
from ..exceptions import PynencError


if TYPE_CHECKING:
    from ..call import Call
    from ..app import Pynenc


class SynchronousInvocation(BaseInvocation[Params, Result]):
    def __init__(self, call: "Call[Params, Result]", result: "Result") -> None:
        super().__init__(call)
        self._result = result

    @cached_property
    def result(self) -> "Result":
        return self._result

    def to_json(self) -> str:
        raise PynencError("SynchronousInvocation cannot be serialized")

    @classmethod
    def from_json(cls, app: "Pynenc", serialized: str) -> "SynchronousInvocation":
        del app, serialized
        raise PynencError("SynchronousInvocation cannot be deserialized")
