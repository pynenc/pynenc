from __future__ import annotations

from functools import cached_property
from typing import TYPE_CHECKING, Iterator

from ..exceptions import PynencError
from ..types import Params, Result
from .base_invocation import BaseInvocation, BaseInvocationGroup

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..call import Call


class SynchronousInvocation(BaseInvocation[Params, Result]):
    def __init__(self, call: Call[Params, Result], result: Result) -> None:
        super().__init__(call)
        self._result = result

    @cached_property
    def result(self) -> Result:
        return self._result

    def to_json(self) -> str:
        raise PynencError("SynchronousInvocation cannot be serialized")

    @classmethod
    def from_json(cls, app: Pynenc, serialized: str) -> SynchronousInvocation:
        del app, serialized
        raise PynencError("SynchronousInvocation cannot be deserialized")


class SynchronousInvocationGroup(
    BaseInvocationGroup[Params, Result, SynchronousInvocation]
):
    @property
    def results(self) -> Iterator[Result]:
        for invocation in self.invocations:
            yield invocation.result
