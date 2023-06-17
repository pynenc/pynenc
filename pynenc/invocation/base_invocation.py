from __future__ import annotations
from abc import ABC, abstractmethod
from typing import Any, Generic

from ..types import Result


class BaseInvocation(ABC, Generic[Result]):
    """"""

    arguments: dict[str, Any]

    @property
    @abstractmethod
    def value(self) -> "Result":
        """"""
