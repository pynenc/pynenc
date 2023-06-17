from __future__ import annotations
from typing import Any, TYPE_CHECKING, TypeVar, ParamSpec

from .base_invocation import BaseInvocation

if TYPE_CHECKING:
    P = ParamSpec("P")
else:
    P = TypeVar("P")
R = TypeVar("R")


class SynchronousInvocation(BaseInvocation[R]):
    """The SynchronousInvocation class is used only for dev"""

    def __init__(self, value: R, arguments: dict[str, Any]) -> None:
        self._value = value
        self.arguments = arguments

    @property
    def value(self) -> R:
        return self._value
