from collections import defaultdict
from typing import TYPE_CHECKING, Any

from pynenc.state_backend.base_state_backend import InvocationHistory

from .base_state_backend import BaseStateBackend

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result


class MemStateBackend(BaseStateBackend):
    def __init__(self, app: "Pynenc") -> None:
        self._cache: dict[str, "DistributedInvocation"] = {}
        self._history: dict[str, list] = defaultdict(list)
        self._results: dict[str, Any] = {}
        self._exceptions: dict[str, Exception] = {}
        super().__init__(app)

    def purge(self) -> None:
        self._cache.clear()
        self._history.clear()
        self._results.clear()
        self._exceptions.clear()

    def _upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        self._cache[invocation.invocation_id] = invocation

    def _get_invocation(
        self, invocation_id: str
    ) -> "DistributedInvocation[Params, Result]":
        return self._cache[invocation_id]

    def _add_history(
        self,
        invocation: "DistributedInvocation",
        invocation_history: "InvocationHistory",
    ) -> None:
        self._history[invocation.invocation_id].append(invocation_history)

    def _get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        return self._history[invocation.invocation_id]

    def _set_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        self._results[invocation.invocation_id] = result

    def _set_exception(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        exception: "Exception",
    ) -> None:
        self._exceptions[invocation.invocation_id] = exception

    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        return self._results[invocation.invocation_id]

    def _get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> Exception:
        return self._exceptions[invocation.invocation_id]
