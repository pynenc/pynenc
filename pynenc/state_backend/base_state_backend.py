import json
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError
from pynenc.invocation.status import InvocationStatus

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.types import Params, Result


@dataclass
class InvocationHistory:
    """
    Data structure representing the history of a task invocation.

    Stores the invocation ID, timestamp, status, and execution context.
    Provides methods for serialization and deserialization to and from JSON.

    :ivar invocation_id: Unique identifier of the invocation.
    :ivar _timestamp: Timestamp of the invocation history creation.
    :ivar status: Current status of the invocation.
    :ivar execution_context: Context of the execution, reserved for future use.
    """

    invocation_id: str
    _timestamp: datetime = field(init=False, default_factory=lambda: datetime.utcnow())
    status: Optional["InvocationStatus"] = None
    execution_context: Optional[Any] = None  # Todo on Runners

    @property
    def timestamp(self) -> datetime:
        """Returns the timestamp of the invocation history."""
        return self._timestamp

    def to_json(self) -> str:
        """
        Serializes the invocation history to a JSON string.

        :return: A JSON representation of the invocation history.
        """
        return json.dumps(
            {
                "invocation_id": self.invocation_id,
                "_timestamp": self._timestamp.isoformat(),
                "status": self.status.value if self.status else None,
                "execution_context": self.execution_context,  # TODO
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "InvocationHistory":
        """
        Deserializes a JSON string into an InvocationHistory object.

        :param str json_str: JSON string to deserialize.
        :return: An instance of InvocationHistory.
        """
        hist_dict = json.loads(json_str)
        history = cls(hist_dict["invocation_id"])
        history._timestamp = datetime.fromisoformat(hist_dict["_timestamp"])
        history.status = InvocationStatus(hist_dict["status"])
        history.execution_context = hist_dict["execution_context"]
        return history


class BaseStateBackend(ABC):
    """
    Abstract base class for state backends in a distributed task system.

    Manages storage and retrieval of invocation-related data,
    including execution status, history, results, and exceptions.
    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.invocation_threads: dict[str, list[threading.Thread]] = defaultdict(list)

    @cached_property
    def conf(self) -> ConfigStateBackend:
        return ConfigStateBackend(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def wait_for_all_async_operations(self) -> None:
        """
        Waits for all asynchronous operations related to invocation status to complete.
        """
        for invocation_id in self.invocation_threads:
            self.wait_for_invocation_async_operations(invocation_id)

    def wait_for_invocation_async_operations(self, invocation_id: str) -> None:
        """
        Waits for all asynchronous operations for a specific invocation to complete.

        :param str invocation_id: ID of the invocation.
        """
        for thread in self.invocation_threads[invocation_id]:
            thread.join()

    @abstractmethod
    def purge(self) -> None:
        """Purges all store state backend data for the current application"""

    @abstractmethod
    def _upsert_invocation(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> None:
        """
        Updates or inserts an invocation.

        :param DistributedInvocation invocation: The invocation to upsert.
        """

    @abstractmethod
    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        """Retrieves an invocation by its ID."""

    @abstractmethod
    def _add_history(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        invocation_history: InvocationHistory,
    ) -> None:
        """Adds a history record for an invocation."""

    @abstractmethod
    def _get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        """
        Retrieves the history of an invocation.

        :param DistributedInvocation invocation: The invocation to get the history from
        """

    @abstractmethod
    def _get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        """
        Retrieves the result of an invocation.

        :param DistributedInvocation invocation: The invocation to get the result from
        """

    @abstractmethod
    def _set_result(
        self, invocation: "DistributedInvocation[Params, Result]", result: "Result"
    ) -> None:
        """
        Sets the result of an invocation.

        :param DistributedInvocation invocation: The invocation to set
        :param Result result: The result to set
        """

    @abstractmethod
    def _get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Exception":
        """
        Retrieves the exception of an invocation.

        :param DistributedInvocation invocation: The invocation to get the exception from
        """

    @abstractmethod
    def _set_exception(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        exception: "Exception",
    ) -> None:
        """
        Sets the raised exception by an invocation ran.

        :param DistributedInvocation invocation: The invocation to set
        :param Exception exception: The exception raised
        """

    def upsert_invocation(self, invocation: "DistributedInvocation") -> None:
        """
        Starts an asynchronous operation to update or insert an invocation.

        :param DistributedInvocation invocation: The invocation to upsert.
        """
        thread = threading.Thread(target=self._upsert_invocation, args=(invocation,))
        thread.start()
        self.invocation_threads[invocation.invocation_id].append(thread)

    def get_invocation(self, invocation_id: str) -> "DistributedInvocation":
        """
        Retrieves an invocation by its ID, raising an error if not found.

        :param DistributedInvocation invocation_id: ID of the invocation.
        :return: The retrieved invocation.
        :raises InvocationNotFoundError: If the invocation wasn't found.
        """
        if invocation := self._get_invocation(invocation_id):
            return invocation
        raise InvocationNotFoundError(invocation_id, "The invocation wasn't stored")

    def add_history(
        self,
        invocation: "DistributedInvocation[Params, Result]",
        status: Optional["InvocationStatus"] = None,
        execution_context: Optional["Any"] = None,
    ) -> None:
        """
        Adds a history record for an invocation.

        :param DistributedInvocation invocation: The invocation to add history for.
        :param Optional[InvocationStatus] status: The status of the invocation.
        :param Optional[Any] execution_context: The execution context of the invocation.
        """
        invocation_history = InvocationHistory(
            invocation.invocation_id, status, execution_context
        )
        thread = threading.Thread(
            target=self._add_history, args=(invocation, invocation_history)
        )
        self.invocation_threads[invocation.invocation_id].append(thread)
        thread.start()

    def get_history(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> list[InvocationHistory]:
        """
        Retrieves the history of an invocation.

        :param DistributedInvocation invocation: The invocation to retrieve history for.
        :return: A list of invocation history records.
        """
        # todo fork open threads
        return self._get_history(invocation)

    def set_result(self, invocation: "DistributedInvocation", result: "Result") -> None:
        """
        Sets the result of an invocation.

        :param DistributedInvocation invocation: The invocation to set the result for.
        :param Result result: The result of the invocation.
        """
        self._set_result(invocation, result)

    def get_result(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> "Result":
        """
        Retrieves the result of an invocation.

        :param DistributedInvocation invocation: The invocation to retrieve the result for.
        :return: The result of the invocation.
        """
        # insert result is block, no need for thread control
        return self._get_result(invocation)

    def set_exception(
        self, invocation: "DistributedInvocation", exception: "Exception"
    ) -> None:
        """
        Sets the exception of an invocation.

        :param DistributedInvocation invocation: The invocation to set the exception for.
        :param Exception exception: The exception of the invocation.
        """
        self._set_exception(invocation, exception)

    def get_exception(
        self, invocation: "DistributedInvocation[Params, Result]"
    ) -> Exception:
        """
        Retrieves the exception of an invocation.

        :param DistributedInvocation invocation: The invocation to retrieve the exception for.
        :return: The exception of the invocation.
        """
        return self._get_exception(invocation)
