from typing import TYPE_CHECKING

from .base_state_backend import BaseStateBackend

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Result


class MemStateBackend(BaseStateBackend):
    def __init__(self, app: "Pynenc") -> None:
        self._cache: dict[str, "DistributedInvocation"] = {}
        super().__init__(app)

    def insert_invocation(self, invocation: "DistributedInvocation") -> None:
        self._cache[invocation.invocation_id] = invocation
