from collections import defaultdict
from typing import TYPE_CHECKING, Any

from pynenc.state_backend.base_state_backend import BaseStateBackend

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.types import Params, Result


class MemStateBackend(BaseStateBackend):
    """
    A memory-based implementation of the state backend.

    Stores invocation data, history, results, and exceptions in in-memory dictionaries.
    Useful for environments where persistence is not required or for testing purposes.

    ```{warning}
    The `MemStateBackend` class stores all data in the process's memory and is not suitable
    for production systems. Its use should be limited to testing or demonstration purposes only.
    ```
    """

    def __init__(self, app: "Pynenc") -> None:
        self._cache: dict[str, "DistributedInvocation"] = {}
        self._history: dict[str, list] = defaultdict(list)
        self._results: dict[str, Any] = {}
        self._exceptions: dict[str, Exception] = {}
        super().__init__(app)

    def purge(self) -> None:
        """Clears all stored data"""
        self._cache.clear()
        self._history.clear()
        self._results.clear()
        self._exceptions.clear()

    def _upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Inserts or updates an invocation in the memory cache.

        :param DistributedInvocation invocation: The invocation object to upsert.
        """
        self._cache[invocation.invocation_id] = invocation

    def _get_invocation(
        self, invocation_id: str
    ) -> "DistributedInvocation[Params, Result]":
        """
        Retrieves an invocation from the memory cache by its ID.

        :param str invocation_id: The ID of the invocation to retrieve.
        :return: The retrieved invocation object.
        """
        return self._cache[invocation_id]

    def _add_history(
        self,
        invocation: "DistributedInvocation",
        invocation_history: "InvocationHistory",
    ) -> None:
        """
        Adds a history record to an invocation.

        :param DistributedInvocation invocation: The invocation to add history for.
        :param InvocationHistory invocation_history: The history record to add.
        """
        self._history[invocation.invocation_id].append(invocation_history)

    def _get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list["InvocationHistory"]:
        """
        Retrieves the history of an invocation.

        :param DistributedInvocation invocation: The invocation to get the history for.
        :return: A list of invocation history records.
        """
        return self._history[invocation.invocation_id]

    def _set_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        """
        Sets the result for an invocation.

        :param DistributedInvocation invocation: The invocation to set the result for.
        :param Result result: The result of the invocation.
        """
        self._results[invocation.invocation_id] = result

    def _set_exception(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        exception: "Exception",
    ) -> None:
        """
        Sets the exception for an invocation.

        :param DistributedInvocation invocation: The invocation to set the exception for.
        :param Exception exception: The exception to set.
        """
        self._exceptions[invocation.invocation_id] = exception

    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        """
        Retrieves the result of an invocation.

        :param DistributedInvocation invocation: The invocation to get the result for.
        :return: The result of the invocation.
        """
        return self._results[invocation.invocation_id]

    def _get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> Exception:
        """
        Retrieves the exception of an invocation.

        :param DistributedInvocation invocation: The invocation to get the exception for.
        :return: The exception of the invocation.
        """
        return self._exceptions[invocation.invocation_id]
