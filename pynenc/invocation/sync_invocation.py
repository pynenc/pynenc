from __future__ import annotations
from typing import TYPE_CHECKING

from .base_invocation import BaseInvocation

if TYPE_CHECKING:
    from ..types import Result, Args


class SynchronousInvocation(BaseInvocation["Result"]):
    """The SynchronousInvocation class is used only for dev"""

    def __init__(self, value: "Result", arguments: "Args") -> None:
        self._value = value
        self.arguments = arguments

    @property
    def value(self) -> "Result":
        return self._value
