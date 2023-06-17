from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result


class BaseStateBackend(ABC):
    def __init__(self, app: "Pynenc") -> None:
        self.app = app

    @abstractmethod
    def insert_invocation(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        ...
