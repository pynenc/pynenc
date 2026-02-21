import json
import threading
from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterator
from dataclasses import dataclass, field
from datetime import UTC, datetime
from functools import cached_property
from typing import TYPE_CHECKING, Any, Generic

from pynenc.conf.config_state_backend import ConfigStateBackend
from pynenc.exceptions import InvocationNotFoundError, PynencError
from pynenc.invocation.dist_invocation import DistributedInvocation
from pynenc.invocation.status import InvocationStatus, InvocationStatusRecord
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc
    from pynenc.call import Call
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.invocation.dist_invocation import InvocationDTO
    from pynenc.models.call_dto import CallDTO
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.identifiers.task_id import TaskId
    from pynenc.workflow import WorkflowIdentity


@dataclass
class InvocationHistory:
    """
    Data structure representing the history of a task invocation.

    Stores the invocation ID, timestamp, status, and execution context.
    Provides methods for serialization and deserialization to and from JSON.

    :ivar str invocation_id: The ID of the invocation this history belongs to
    :ivar _timestamp: Timestamp of the invocation history creation.
    :ivar InvocationStatusRecord status_record: Current status of the invocation.
    :ivar RunnerContext owner_context: Context of the execution.
    """

    invocation_id: str
    _timestamp: datetime = field(init=False, default_factory=lambda: datetime.now(UTC))
    status_record: InvocationStatusRecord
    runner_context_id: str
    registered_by_inv_id: str | None = None

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
                "status_record": self.status_record.to_json(),
                "runner_context_id": self.runner_context_id,
                "registered_by_inv_id": self.registered_by_inv_id,
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
        history = cls(
            invocation_id=hist_dict["invocation_id"],
            status_record=InvocationStatusRecord.from_json(hist_dict["status_record"]),
            runner_context_id=hist_dict["runner_context_id"],
            registered_by_inv_id=hist_dict.get("registered_by_inv_id"),
        )

        timestamp = datetime.fromisoformat(hist_dict["_timestamp"])
        if timestamp.tzinfo is None:
            timestamp = timestamp.replace(tzinfo=UTC)
        history._timestamp = timestamp

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
        self._runner_context_cache: dict[str, RunnerContext] = {}

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
    def _get_invocation(
        self, invocation_id: "InvocationId"
    ) -> tuple["InvocationDTO", "CallDTO"] | None:
        """Retrieve invocation and call DTOs by invocation ID.

        :param "InvocationId" invocation_id: The ID of the invocation to retrieve.
        :return: Paired DTOs if found, else None.
        """

    @abstractmethod
    def _add_histories(
        self,
        invocation_ids: list["InvocationId"],
        invocation_history: "InvocationHistory",
    ) -> None:
        """
        Adds a history record for a list of invocations.

        :param list["InvocationId"] invocation_ids: The IDs of the invocations.
        :param InvocationHistory invocation_history: The history record to add.
        """

    @abstractmethod
    def _get_history(self, invocation_id: "InvocationId") -> list["InvocationHistory"]:
        """
        Retrieves the history of an invocation ordered by timestamp.

        :param "InvocationId" invocation_id: The ID of the invocation to get the history from
        :return: List of InvocationHistory records
        """

    @abstractmethod
    def _get_result(self, invocation_id: "InvocationId") -> str:
        """
        Retrieves the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to get the result from
        :return: The serialized result string
        """

    @abstractmethod
    def _set_result(
        self, invocation_id: "InvocationId", serialized_result: str
    ) -> None:
        """
        Sets the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to set
        :param str serialized_result: The serialized result to set
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
            serialized_exception["error_data"] = self.app.client_data_store.serialize(
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
        return self.app.client_data_store.deserialize(
            serialized_exception["error_data"]
        )

    @abstractmethod
    def _get_exception(self, invocation_id: "InvocationId") -> str:
        """
        Retrieves the serialized exception of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to get the exception from
        :return: The serialized exception string
        """

    @abstractmethod
    def _set_exception(
        self, invocation_id: "InvocationId", serialized_exception: str
    ) -> None:
        """
        Sets the raised exception by an invocation ran.

        :param "InvocationId" invocation_id: The ID of the invocation to set
        :param "Exception" exception: The exception raised
        """

    def upsert_invocations(self, invocations: list["DistributedInvocation"]) -> None:
        """Persist invocations by converting to DTOs and storing via backend.

        Converts each invocation into an ``(InvocationDTO, CallDTO)`` pair and
        passes them to the abstract ``_upsert_invocations``.  Argument
        serialization and external-storage decisions are handled transparently
        by ``Call.serialized_arguments`` (which delegates to
        ``ClientDataStore.serialize_arguments``).

        :param list[DistributedInvocation] invocations: The invocations to upsert.
        """
        pairs: list[tuple[InvocationDTO, CallDTO]] = []
        for invocation in invocations:
            inv_dto = invocation.to_dto()
            call_dto = invocation.call.to_dto()
            pairs.append((inv_dto, call_dto))
        self._upsert_invocations(pairs)

    @abstractmethod
    def _upsert_invocations(
        self, entries: list[tuple["InvocationDTO", "CallDTO"]]
    ) -> None:
        """Backend-specific storage of invocation and call DTOs.

        Each entry is a paired ``(InvocationDTO, CallDTO)`` ready for persistence.
        Backends can store them together or in separate tables/collections.

        :param list[tuple[InvocationDTO, CallDTO]] entries: Paired DTOs to persist.
        """

    def get_invocation(self, invocation_id: "InvocationId") -> "DistributedInvocation":
        """Retrieve an invocation by ID, reconstructing from DTOs.

        Loads the ``InvocationDTO`` and ``CallDTO`` from the backend, constructs
        a ``LazyCall`` (arguments deserialized only when accessed), and
        reassembles the ``DistributedInvocation`` via ``from_dto``.

        :param "InvocationId" invocation_id: ID of the invocation.
        :return: The retrieved invocation with a LazyCall.
        :raises InvocationNotFoundError: If the invocation wasn't found.
        """
        from pynenc.call import LazyCall
        from pynenc.invocation.dist_invocation import DistributedInvocation

        result = self._get_invocation(invocation_id)
        if result is None:
            raise InvocationNotFoundError(invocation_id, "The invocation wasn't stored")
        inv_dto, call_dto = result
        lazy_call: Call = LazyCall.from_dto(self.app, call_dto)
        return DistributedInvocation.from_dto(inv_dto, call=lazy_call)

    def add_histories(
        self,
        invocations: list["DistributedInvocation[Params, Result]"],
        status_record: "InvocationStatusRecord",
        runner_context: "RunnerContext",
    ) -> None:
        """
        Adds a history record for invocations.

        :param list[DistributedInvocation] invocations: The invocations to add history for.
        :param InvocationStatusRecord status_record: The status record of the invocation.
        :param RunnerContext owner_context: The owner context of the execution.
        """
        self.store_runner_context(runner_context)
        for invocation in invocations:
            registered_by_inv_id = (
                invocation.parent_invocation_id
                if status_record.status == InvocationStatus.REGISTERED
                else None
            )
            invocation_history = InvocationHistory(
                invocation_id=invocation.invocation_id,
                status_record=status_record,
                runner_context_id=runner_context.runner_id,
                registered_by_inv_id=registered_by_inv_id,
            )
            thread = threading.Thread(
                target=self._add_histories,
                args=([invocation.invocation_id], invocation_history),
            )
            self.invocation_threads[invocation.invocation_id].append(thread)
            thread.start()

    def add_history(
        self,
        invocation_id: "InvocationId",
        status_record: "InvocationStatusRecord",
        runner_context: "RunnerContext",
    ) -> None:
        """
        Adds a history record for a single invocation.

        :param "InvocationId" invocation_id: The ID of the invocation.
        :param InvocationStatusRecord status_record: The status record.
        :param RunnerContext runner_context: The runner context.
        """
        self.store_runner_context(runner_context)
        invocation_history = InvocationHistory(
            invocation_id=invocation_id,
            status_record=status_record,
            runner_context_id=runner_context.runner_id,
        )
        thread = threading.Thread(
            target=self._add_histories, args=([invocation_id], invocation_history)
        )
        self.invocation_threads[invocation_id].append(thread)
        thread.start()

    def get_history(self, invocation_id: "InvocationId") -> list[InvocationHistory]:
        """
        Retrieves the history of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to retrieve history for.
        :return: A list of invocation history records.
        """
        return self._get_history(invocation_id)

    def set_result(self, invocation_id: "InvocationId", result: Result) -> None:
        """
        Sets the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to set the result for.
        :param Result result: The result of the invocation.
        """
        serialized_result = self.app.client_data_store.serialize(result)
        self._set_result(invocation_id, serialized_result)

    def get_result(self, invocation_id: "InvocationId") -> Result:
        """
        Retrieves the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to retrieve the result for.
        :return: The result of the invocation.
        """
        # insert result is block, no need for thread control
        serialized_result = self._get_result(invocation_id)
        return self.app.client_data_store.deserialize(serialized_result)

    def set_exception(
        self, invocation_id: "InvocationId", exception: "Exception"
    ) -> None:
        """
        Sets the exception of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to set the exception for.
        :param Exception exception: The exception of the invocation.
        """
        serialized_exception = self.serialize_exception(exception)
        self._set_exception(invocation_id, serialized_exception)

    def get_exception(self, invocation_id: "InvocationId") -> Exception:
        """
        Retrieves the exception of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to retrieve the exception for.
        :return: The exception of the invocation.
        """
        serialized_exception = self._get_exception(invocation_id)
        return self.deserialize_exception(serialized_exception)

    @abstractmethod
    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Set a value in workflow data.

        :param "WorkflowIdentity" workflow_identity: Workflow identity
        :param str key: Data key to set
        :param Any value: Value to store
        """

    @abstractmethod
    def get_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, default: Any = None
    ) -> Any:
        """
        Get a value from workflow data.

        :param "WorkflowIdentity" workflow_identity: Workflow identity
        :param str key: Data key to retrieve
        :param Any default: Default value if key doesn't exist
        :return: Stored value or default
        """

    @abstractmethod
    def store_app_info(self, app_info: "AppInfo") -> None:
        """
        Register this app's information in the state backend for discovery.

        :param "AppInfo" app_info: The app information to store
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
    def get_all_workflow_types(self) -> Iterator["TaskId"]:
        """
        Retrieve all workflow types (workflow_task_ids) stored in this state backend.

        :return: Iterator of workflow task IDs representing different workflow types
        """

    @abstractmethod
    def get_all_workflow_runs(self) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :return: Iterator of workflow identities for runs
        """

    @abstractmethod
    def get_workflow_runs(
        self, workflow_type: "TaskId"
    ) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :param workflow_type: Filter for specific workflow type
        :return: Iterator of workflow identities for runs
        """

    @abstractmethod
    def store_workflow_sub_invocation(
        self, parent_workflow_id: "InvocationId", sub_invocation_id: "InvocationId"
    ) -> None:
        """
        Store a sub-invocation ID that runs inside a parent workflow.

        This tracks which invocations (tasks or sub-workflows) are executed
        within the context of a parent workflow for monitoring and debugging.

        :param "InvocationId" parent_workflow_id: The workflow ID that contains the sub-invocation
        :param sub_invocation_id: The invocation ID of the task/sub-workflow running inside
        """

    @abstractmethod
    def get_workflow_sub_invocations(
        self, workflow_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
        """
        Retrieve all sub-invocation IDs that run inside a specific workflow.

        :param workflow_id: The workflow ID to get sub-invocations for
        :return: Iterator of invocation IDs that run inside the workflow
        """

    @abstractmethod
    def iter_invocations_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationId"]]:
        """
        Iterate over invocation IDs that have history within a time range.

        :param datetime start_time: Start of the time range
        :param datetime end_time: End of the time range
        :param int batch_size: Number of invocation IDs per batch
        :return: Iterator yielding batches of invocation IDs
        """

    @abstractmethod
    def iter_history_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationHistory"]]:
        """
        Iterate over history entries within a time range.

        :param datetime start_time: Start of the time range
        :param datetime end_time: End of the time range
        :param int batch_size: Number of history entries per batch
        :return: Iterator yielding batches of InvocationHistory
        """

    @abstractmethod
    def _store_runner_context(self, runner_context: "RunnerContext") -> None:
        """
        Backend-specific implementation to store a runner context.

        :param RunnerContext runner_context: The context to store
        """

    def store_runner_context(self, runner_context: "RunnerContext") -> None:
        """
        Store a runner context.

        Updates the local cache and delegates to backend implementation.

        :param RunnerContext runner_context: The context to store
        """
        if runner_context.runner_id not in self._runner_context_cache:
            self._store_runner_context(runner_context)
        self._runner_context_cache[runner_context.runner_id] = runner_context
        if runner_context.parent_ctx:
            self.store_runner_context(runner_context.parent_ctx)

    @abstractmethod
    def _get_runner_context(self, runner_id: str) -> "RunnerContext | None":
        """
        Backend-specific implementation to retrieve a single runner context.

        :param str runner_id: The runner's unique identifier
        :return: The stored RunnerContext or None if not found
        """

    @abstractmethod
    def _get_runner_contexts(self, runner_ids: list[str]) -> list["RunnerContext"]:
        """
        Backend-specific implementation to retrieve multiple runner contexts.

        Only called for IDs not found in local cache.

        :param list[str] runner_ids: List of runner unique identifiers
        :return: list["RunnerContext"] of the stored RunnerContexts
        """

    def get_runner_context(self, runner_id: str) -> "RunnerContext | None":
        """
        Retrieve a runner context by runner_id.

        Checks local cache first, then backend if not found.

        :param str runner_id: The runner's unique identifier
        :return: The stored RunnerContext or None if not found
        """
        if runner_id in self._runner_context_cache:
            return self._runner_context_cache[runner_id]

        # Try to fetch from backend
        contexts = self._get_runner_contexts([runner_id])
        if contexts:
            ctx = contexts[0]
            self._runner_context_cache[runner_id] = ctx
            return ctx
        return None

    def get_runner_contexts(self, runner_ids: list[str]) -> list["RunnerContext"]:
        """
        Retrieve multiple runner contexts by their IDs.

        Uses local cache for known contexts, fetches missing ones from backend,
        and updates cache with newly fetched contexts.

        :param list[str] runner_ids: List of runner unique identifiers
        :return: list["RunnerContext"] of the stored RunnerContexts
        """
        if not runner_ids:
            return []

        # Check cache first
        cached_contexts = []
        missing_ids = []

        for runner_id in runner_ids:
            if runner_id in self._runner_context_cache:
                cached_contexts.append(self._runner_context_cache[runner_id])
            else:
                missing_ids.append(runner_id)

        # If all found in cache, return immediately
        if not missing_ids:
            return cached_contexts

        # Fetch missing ones from backend
        fetched_contexts = self._get_runner_contexts(missing_ids)

        # Update cache with fetched contexts
        for ctx in fetched_contexts:
            self._runner_context_cache[ctx.runner_id] = ctx

        # Return all found contexts
        return cached_contexts + fetched_contexts
