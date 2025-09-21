import json
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from dataclasses import dataclass, field
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic, Iterator, Optional

from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError, PynencError
from pynenc.invocation.status import InvocationStatus
from pynenc.runner import RunnerContext
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.workflow import WorkflowIdentity


@dataclass
class InvocationHistory:
    """
    Data structure representing the history of a task invocation.

    Stores the invocation ID, timestamp, status, and execution context.
    Provides methods for serialization and deserialization to and from JSON.

    :ivar _timestamp: Timestamp of the invocation history creation.
    :ivar status: Current status of the invocation.
    :ivar execution_context: Context of the execution, reserved for future use.
    """

    _timestamp: datetime = field(
        init=False, default_factory=lambda: datetime.now(timezone.utc)
    )
    status: InvocationStatus
    runner_context: Optional[RunnerContext] = None

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
                "_timestamp": self._timestamp.isoformat(),
                "status": self.status.value,
                "runner_context": self.runner_context.to_json()
                if self.runner_context
                else None,
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
        history = cls(InvocationStatus(hist_dict["status"]))

        timestamp = datetime.fromisoformat(hist_dict["_timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=timezone.utc)
        history._timestamp = timestamp

        if runner_ctx_json := hist_dict.get("runner_context"):
            history.runner_context = RunnerContext.from_json(runner_ctx_json)

        return history


class BaseStateBackend(ABC, Generic[Params, Result]):
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
    def _upsert_invocations(
        self, invocations: list["DistributedInvocation[Params, Result]"]
    ) -> None:
        """
        Updates or inserts multiple invocations.

        :param list[DistributedInvocation] invocations: The invocations to upsert.
        """

    @abstractmethod
    def _get_invocation(self, invocation_id: str) -> Optional["DistributedInvocation"]:
        """
        Retrieves an invocation by its ID.

        :param str invocation_id: The ID of the invocation to retrieve.
        :return: The invocation object if found, else None.
        """

    @abstractmethod
    def _add_histories(
        self, invocation_ids: list[str], invocation_history: "InvocationHistory"
    ) -> None:
        """
        Adds a histories record for a list of invocations.

        :param list[str] invocation_ids: The IDs of the invocations.
        :param InvocationHistory invocation_history: The history record to add.
        """

    @abstractmethod
    def _get_history(self, invocation_id: str) -> list["InvocationHistory"]:
        """
        Retrieves the history of an invocation ordered by timestamp.

        :param str invocation_id: The ID of the invocation to get the history from
        :return: List of InvocationHistory records
        """

    @abstractmethod
    def _get_result(self, invocation_id: str) -> Result:
        """
        Retrieves the result of an invocation.

        :param str invocation_id: The ID of the invocation to get the result from
        :return: The result value
        """

    @abstractmethod
    def _set_result(self, invocation_id: str, result: Result) -> None:
        """
        Sets the result of an invocation.

        :param str invocation_id: The ID of the invocation to set
        :param Result result: The result to set
        """

    def serialize_exception(self, exception: Exception) -> str:
        """
        Serializes an exception into a string representation.

        This method provides a default implementation for serializing exceptions using the `serialize` method.
        It can be overridden by subclasses if specific handling for exceptions is required.

        :param Exception exception: The exception to be serialized.
        :return: A string representation of the serialized exception.
        """
        serialized_exception: dict[str, str | bool] = {
            "error_name": exception.__class__.__name__
        }
        if isinstance(exception, PynencError):
            serialized_exception["pynenc_error"] = True
            serialized_exception["error_data"] = exception.to_json()
        else:
            serialized_exception["pynenc_error"] = False
            serialized_exception["error_data"] = self.app.serializer.serialize(
                exception
            )
        return json.dumps(serialized_exception)

    def deserialize_exception(self, serialized: str) -> Exception:
        """
        Deserializes a string representation of an exception back into an Exception object.

        This method provides a default implementation for deserializing exceptions using the `deserialize` method.
        It can be overridden by subclasses if specific handling for exceptions is required.

        :param str serialized: The string representation of the serialized exception.
        :return: The deserialized Exception object.
        """
        serialized_exception = json.loads(serialized)
        if serialized_exception["pynenc_error"]:
            return PynencError.from_json(
                serialized_exception["error_name"],
                serialized_exception["error_data"],
            )
        return self.app.serializer.deserialize(serialized_exception["error_data"])

    @abstractmethod
    def _get_exception(self, invocation_id: str) -> "Exception":
        """
        Retrieves the exception of an invocation.

        :param str invocation_id: The ID of the invocation to get the exception from
        :return: The exception object
        """

    @abstractmethod
    def _set_exception(self, invocation_id: str, exception: "Exception") -> None:
        """
        Sets the raised exception by an invocation ran.

        :param str invocation_id: The ID of the invocation to set
        :param Exception exception: The exception raised
        """

    def upsert_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """
        Starts an asynchronous operation to update or insert invocations.

        :param DistributedInvocation invocation: The invocation to upsert.
        """
        self._upsert_invocations(invocations)

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

    def add_histories(
        self,
        invocation_ids: list[str],
        status: "InvocationStatus",
        runner_context: Optional["RunnerContext"] = None,
    ) -> None:
        """
        Adds a history record for an invocation.

        :param str invocation_id: The invocation Id to add history for.
        :param Optional[InvocationStatus] status: The status of the invocation.
        :param Optional[Any] execution_context: The execution context of the invocation.
        """
        invocation_history = InvocationHistory(status, runner_context)
        thread = threading.Thread(
            target=self._add_histories, args=(invocation_ids, invocation_history)
        )
        for invocation_id in invocation_ids:
            self.invocation_threads[invocation_id].append(thread)
        thread.start()

    def get_history(self, invocation_id: str) -> list[InvocationHistory]:
        """
        Retrieves the history of an invocation.

        :param str invocation_id: The ID of the invocation to retrieve history for.

        :return: A list of invocation history records.
        """
        # todo fork open threads
        return self._get_history(invocation_id)

    def set_result(self, invocation_id: str, result: Result) -> None:
        """
        Sets the result of an invocation.

        :param str invocation_id: The ID of the invocation to set the result for.
        :param Result result: The result of the invocation.
        """
        self._set_result(invocation_id, result)

    def get_result(self, invocation_id: str) -> Result:
        """
        Retrieves the result of an invocation.

        :param str invocation_id: The ID of the invocation to retrieve the result for.
        :return: The result of the invocation.
        """
        # insert result is block, no need for thread control
        return self._get_result(invocation_id)

    def set_exception(self, invocation_id: str, exception: "Exception") -> None:
        """
        Sets the exception of an invocation.

        :param str invocation_id: The ID of the invocation to set the exception for.
        :param Exception exception: The exception of the invocation.
        """
        self._set_exception(invocation_id, exception)

    def get_exception(self, invocation_id: str) -> Exception:
        """
        Retrieves the exception of an invocation.

        :param str invocation_id: The ID of the invocation to retrieve the exception for.
        :return: The exception of the invocation.
        """
        return self._get_exception(invocation_id)

    @abstractmethod
    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Set a value in workflow data.

        :param workflow_identity: Workflow identity
        :param key: Data key to set
        :param value: Value to store
        """

    @abstractmethod
    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """
        Get a value from workflow data.

        :param workflow_identity: Workflow identity
        :param key: Data key to retrieve
        :param default: Default value if key doesn't exist
        :return: Stored value or default
        """

    @abstractmethod
    def store_app_info(self, app_info: "AppInfo") -> None:
        """
        Register this app's information in the state backend for discovery.

        :param app_info: The app information to store
        :return: None
        """

    @abstractmethod
    def get_app_info(self) -> "AppInfo":
        """
        Retrieve information of the current app.
        :return: The app information
        """

    @staticmethod
    @abstractmethod
    def discover_app_infos() -> dict[str, "AppInfo"]:
        """
        This static method tries to discover all registered apps in the system.
        We will try to connect to the backend with the default configuration.

        :return: Dictionary mapping app_id to app information
        """

    @abstractmethod
    def store_workflow_run(self, workflow_identity: "WorkflowIdentity") -> None:
        """
        Store a workflow run for tracking and monitoring.

        Maintains workflow type registry and specific workflow run instances.
        This enables monitoring of workflow types and their execution history.

        :param workflow_identity: The workflow identity to store
        """

    @abstractmethod
    def get_all_workflows(self) -> Iterator[str]:
        """
        Retrieve all workflow types (workflow_task_ids) stored in this state backend.

        :return: Iterator of workflow task IDs representing different workflow types
        """

    @abstractmethod
    def get_all_workflows_runs(self) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :return: Iterator of workflow identities for runs
        """

    @abstractmethod
    def get_workflow_runs(self, workflow_type: str) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :param workflow_type: Filter for specific workflow type
        :return: Iterator of workflow identities for runs
        """

    @abstractmethod
    def store_workflow_sub_invocation(
        self, parent_workflow_id: str, sub_invocation_id: str
    ) -> None:
        """
        Store a sub-invocation ID that runs inside a parent workflow.

        This tracks which invocations (tasks or sub-workflows) are executed
        within the context of a parent workflow for monitoring and debugging.

        :param parent_workflow_id: The workflow ID that contains the sub-invocation
        :param sub_invocation_id: The invocation ID of the task/sub-workflow running inside
        """

    @abstractmethod
    def get_workflow_sub_invocations(self, workflow_id: str) -> Iterator[str]:
        """
        Retrieve all sub-invocation IDs that run inside a specific workflow.

        :param workflow_id: The workflow ID to get sub-invocations for
        :return: Iterator of invocation IDs that run inside the workflow
        """
