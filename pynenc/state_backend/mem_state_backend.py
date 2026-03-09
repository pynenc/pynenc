import itertools
from collections import defaultdict
from collections.abc import Iterator
from datetime import datetime
from typing import TYPE_CHECKING, Any

from pynenc.state_backend.base_state_backend import BaseStateBackend
from pynenc.identifiers.invocation_id import InvocationId as InvId
from pynenc.identifiers.task_id import TaskId
from pynenc.types import Params, Result

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.invocation.dist_invocation import InvocationDTO
    from pynenc.models.call_dto import CallDTO
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.workflow import WorkflowIdentity


_APP_INFO_REGISTRY: dict[str, "AppInfo"] = {}


class MemStateBackend(BaseStateBackend[Params, Result]):
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
        self._cache: dict[str, tuple[InvocationDTO, CallDTO]] = {}
        self._parent_to_children: dict[str, list[str]] = defaultdict(list)
        self._runner_contexts: dict[str, RunnerContext] = {}
        self._history: dict[InvocationId, list] = defaultdict(list)
        self._results: dict[InvocationId, str] = {}
        self._exceptions: dict[InvocationId, str] = {}
        self._workflow_data: dict[InvocationId, dict[str, Any]] = defaultdict(dict)
        self._workflow_types: set[TaskId] = set()  # Stores workflow_task_ids
        self._workflow_runs: dict[TaskId, set[WorkflowIdentity]] = defaultdict(
            set
        )  # workflow_task_id -> runs
        self._workflow_sub_invocations: dict[InvocationId, set[InvocationId]] = (
            defaultdict(set)
        )  # workflow_id -> sub_invocation_ids
        super().__init__(app)

    def purge(self) -> None:
        """Clears all stored data"""
        self._cache.clear()
        self._parent_to_children.clear()
        self._history.clear()
        self._results.clear()
        self._exceptions.clear()
        self._workflow_types.clear()
        self._workflow_runs.clear()
        self._workflow_sub_invocations.clear()

    def _upsert_invocations(
        self, entries: list[tuple["InvocationDTO", "CallDTO"]]
    ) -> None:
        """Store invocation and call DTO pairs in the memory cache.

        Also maintains the parent-to-children index for efficient family
        tree traversal without scanning the entire cache.
        """
        for inv_dto, call_dto in entries:
            self._cache[inv_dto.invocation_id] = (inv_dto, call_dto)
            if inv_dto.parent_invocation_id is not None:
                parent_key = str(inv_dto.parent_invocation_id)
                child_key = str(inv_dto.invocation_id)
                children = self._parent_to_children[parent_key]
                if child_key not in children:
                    children.append(child_key)

    def _get_invocation(
        self, invocation_id: "InvocationId"
    ) -> tuple["InvocationDTO", "CallDTO"] | None:
        """Retrieve an invocation DTO pair from the memory cache.

        :param "InvocationId" invocation_id: The ID of the invocation to retrieve.
        :return: Paired DTOs if found, else None.
        """
        return self._cache.get(invocation_id)

    def get_child_invocations(
        self, parent_invocation_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
        """Return IDs of invocations that name the given ID as their parent.

        Uses the pre-built parent-to-children index instead of scanning the
        entire cache, providing O(1) lookup per parent.

        :param parent_invocation_id: The parent invocation ID to search for.
        :return: Iterator of child invocation IDs.
        """
        parent_key = str(parent_invocation_id)
        return (
            InvId(child_key)
            for child_key in self._parent_to_children.get(parent_key, [])
        )

    def _add_histories(
        self,
        invocation_ids: list["InvocationId"],
        invocation_history: "InvocationHistory",
    ) -> None:
        """Adds the same history record for a list of invocations."""
        for invocation_id in invocation_ids:
            self._history[invocation_id].append(invocation_history)

    def _get_history(self, invocation_id: "InvocationId") -> list["InvocationHistory"]:
        """
        Retrieves the history of an invocation ordered by timestamp.

        :param "InvocationId" invocation_id: The ID of the invocation to get the history from
        :return: List of InvocationHistory records
        """
        return sorted(
            self._history.get(invocation_id, []),
            key=lambda record: record.timestamp,
        )

    def _get_result(self, invocation_id: "InvocationId") -> str:
        """
        Retrieves the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to get the result from
        :return: The serialized result string
        """
        return self._results[invocation_id]

    def _set_result(
        self, invocation_id: "InvocationId", serialized_result: str
    ) -> None:
        """
        Sets the result of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to set
        :param str serialized_result: The serialized result string to set
        """
        self._results[invocation_id] = serialized_result

    def _get_exception(self, invocation_id: "InvocationId") -> str:
        """
        Retrieves the exception of an invocation.

        :param "InvocationId" invocation_id: The ID of the invocation to get the exception from
        :return: The serialized exception string
        """
        return self._exceptions[invocation_id]

    def _set_exception(
        self, invocation_id: "InvocationId", serialized_exception: str
    ) -> None:
        """
        Sets the raised exception by an invocation ran.

        :param "InvocationId" invocation_id: The ID of the invocation to set
        :param str serialized_exception: The serialized exception string to set
        """
        self._exceptions[invocation_id] = serialized_exception

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
        workflow_id = workflow_identity.workflow_id
        return self._workflow_data.get(workflow_id, {}).get(key, default)

    def set_workflow_data(
        self, workflow_identity: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Set a value in workflow data.

        :param workflow_identity: Workflow identity
        :param key: Data key to set
        :param value: Value to store
        """
        workflow_id = workflow_identity.workflow_id
        if workflow_id not in self._workflow_data:
            self._workflow_data[workflow_id] = {}
        self._workflow_data[workflow_id][key] = value

    def store_app_info(self, app_info: "AppInfo") -> None:
        """
        Register this app's information in the state backend for discovery.

        :param app_info: The app information to store
        """
        _APP_INFO_REGISTRY[app_info.app_id] = app_info

    def get_app_info(self) -> "AppInfo":
        """
        Retrieve information of the current app.

        :return: The app information
        :raises ValueError: If app info is not found
        """
        app_id = self.app.app_id
        if app_id not in _APP_INFO_REGISTRY:
            raise ValueError(f"No app info found for app_id '{app_id}'")
        return _APP_INFO_REGISTRY[app_id]

    @staticmethod
    def discover_app_infos() -> dict[str, "AppInfo"]:
        return _APP_INFO_REGISTRY

    def store_workflow_run(self, workflow_identity: "WorkflowIdentity") -> None:
        """
        Store a workflow run for tracking and monitoring.

        :param workflow_identity: The workflow identity to store
        """
        self._workflow_types.add(workflow_identity.workflow_type)
        self._workflow_runs[workflow_identity.workflow_type].add(workflow_identity)

    def get_all_workflow_types(self) -> Iterator["TaskId"]:
        """
        Retrieve all workflow types (workflow_task_ids) stored in this state backend.

        :return: Iterator of workflow task IDs representing different workflow types
        """
        return iter(self._workflow_types)

    def get_all_workflow_runs(self) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :return: Iterator of workflow identities for runs
        """
        return itertools.chain.from_iterable(self._workflow_runs.values())

    def get_workflow_runs(
        self, workflow_type: "TaskId"
    ) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :param workflow_type: Filter for specific workflow type
        :return: Iterator of workflow identities for runs
        """
        return iter(self._workflow_runs.get(workflow_type, []))

    def store_workflow_sub_invocation(
        self, parent_workflow_id: "InvocationId", sub_invocation_id: "InvocationId"
    ) -> None:
        """
        Store a sub-invocation ID that runs inside a parent workflow.

        :param parent_workflow_id: The workflow ID that contains the sub-invocation
        :param sub_invocation_id: The invocation ID of the task/sub-workflow running inside
        """
        self._workflow_sub_invocations[parent_workflow_id].add(sub_invocation_id)

    def get_workflow_sub_invocations(
        self, workflow_id: "InvocationId"
    ) -> Iterator["InvocationId"]:
        """
        Retrieve all sub-invocation IDs that run inside a specific workflow.

        :param workflow_id: The workflow ID to get sub-invocations for
        :return: Iterator of invocation IDs that run inside the workflow
        """
        return iter(self._workflow_sub_invocations.get(workflow_id, set()))

    def iter_invocations_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationId"]]:
        """Iterate over invocation IDs that have history within time range."""
        # Collect all invocation IDs that have history entries in the time range
        matching_ids: set[InvocationId] = set()
        for invocation_id, history_list in self._history.items():
            for history_entry in history_list:
                if start_time <= history_entry.timestamp <= end_time:
                    matching_ids.add(invocation_id)
                    break  # Found match, no need to check more entries

        # Yield in batches
        id_list = sorted(matching_ids)
        for i in range(0, len(id_list), batch_size):
            yield id_list[i : i + batch_size]

    def iter_history_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        batch_size: int = 100,
    ) -> Iterator[list["InvocationHistory"]]:
        """Iterate over history entries within time range."""
        # Collect all history entries in the time range
        matching_entries: list[tuple[datetime, InvocationHistory]] = []
        for history_list in self._history.values():
            for history_entry in history_list:
                if start_time <= history_entry.timestamp <= end_time:
                    matching_entries.append((history_entry.timestamp, history_entry))

        # Sort by timestamp
        matching_entries.sort(key=lambda x: x[0])

        # Yield in batches
        for i in range(0, len(matching_entries), batch_size):
            batch = [entry for _, entry in matching_entries[i : i + batch_size]]
            yield batch

    def _store_runner_context(self, runner_context: "RunnerContext") -> None:
        """
        Store a runner context.

        :param str runner_id: The runner's unique identifier
        :param RunnerContext runner_context: The context to store
        """
        self._runner_contexts[runner_context.runner_id] = runner_context

    def _get_runner_context(self, runner_id: str) -> "RunnerContext | None":
        """
        Retrieve a runner context by runner_id.

        :param str runner_id: The runner's unique identifier
        :return: The stored RunnerContext or None if not found
        """
        return self._runner_contexts.get(runner_id)

    def _get_runner_contexts(self, runner_ids: list[str]) -> list["RunnerContext"]:
        """
        Retrieve multiple runner contexts by their IDs.

        :param list[str] runner_ids: List of runner unique identifiers
        :return: list["RunnerContext"] of the stored RunnerContexts
        """
        return [
            self._runner_contexts[runner_id]
            for runner_id in runner_ids
            if runner_id in self._runner_contexts
        ]

    def get_matching_runner_contexts(
        self, partial_id: str
    ) -> Iterator["RunnerContext"]:
        """Search runner contexts by partial ID match."""
        for rid, ctx in self._runner_contexts.items():
            if partial_id in rid:
                yield ctx

    def get_invocation_ids_by_workflow(
        self,
        workflow_id: str | None = None,
        workflow_type_key: str | None = None,
    ) -> Iterator["InvocationId"]:
        """Retrieve invocation IDs filtered by workflow criteria."""
        for _inv_id, (inv_dto, _call_dto) in self._cache.items():
            wf = inv_dto.workflow
            if workflow_id and str(wf.workflow_id) != workflow_id:
                continue
            if workflow_type_key and wf.workflow_type.key != workflow_type_key:
                continue
            yield inv_dto.invocation_id
