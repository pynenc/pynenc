import itertools
from collections import defaultdict
from typing import TYPE_CHECKING, Any, Iterator

from pynenc.state_backend.base_state_backend import BaseStateBackend

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.state_backend.base_state_backend import InvocationHistory
    from pynenc.types import Params, Result
    from pynenc.workflow import WorkflowIdentity


_APP_INFO_REGISTRY: dict[str, "AppInfo"] = {}


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
        self._cache: dict[str, DistributedInvocation] = {}
        self._history: dict[str, list] = defaultdict(list)
        self._results: dict[str, Any] = {}
        self._exceptions: dict[str, Exception] = {}
        self._workflow_data: dict[str, dict[str, Any]] = defaultdict(dict)
        self._deterministic_values: dict[str, dict[str, Any]] = defaultdict(dict)
        self._workflow_types: set[str] = set()  # Stores workflow_task_ids
        self._workflow_runs: dict[str, list["WorkflowIdentity"]] = defaultdict(
            list
        )  # workflow_task_id -> runs
        self._workflow_sub_invocations: dict[str, set[str]] = defaultdict(
            set
        )  # workflow_id -> sub_invocation_ids
        super().__init__(app)

    def purge(self) -> None:
        """Clears all stored data"""
        self._cache.clear()
        self._history.clear()
        self._results.clear()
        self._exceptions.clear()
        self._workflow_types.clear()
        self._workflow_runs.clear()
        self._workflow_sub_invocations.clear()

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

    def get_workflow_deterministic_value(
        self, workflow: "WorkflowIdentity", key: str
    ) -> Any:
        """
        Retrieve a deterministic value for workflow operations.

        :param workflow: The workflow identity
        :param key: Key identifying the deterministic value
        :return: The stored value or None if not found
        """
        workflow_id = workflow.workflow_id
        return self._deterministic_values.get(workflow_id, {}).get(key)

    def set_workflow_deterministic_value(
        self, workflow: "WorkflowIdentity", key: str, value: Any
    ) -> None:
        """
        Store a deterministic value for workflow operations.

        :param workflow: The workflow identity
        :param key: Key identifying the deterministic value
        :param value: The value to store
        """
        workflow_id = workflow.workflow_id
        if workflow_id not in self._deterministic_values:
            self._deterministic_values[workflow_id] = {}
        self._deterministic_values[workflow_id][key] = value

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
    def get_all_app_infos() -> dict[str, "AppInfo"]:
        return _APP_INFO_REGISTRY

    def store_workflow_run(self, workflow_identity: "WorkflowIdentity") -> None:
        """
        Store a workflow run for tracking and monitoring.

        :param workflow_identity: The workflow identity to store
        """
        self._workflow_types.add(workflow_identity.workflow_task_id)
        self._workflow_runs[workflow_identity.workflow_task_id].append(
            workflow_identity
        )

    def get_all_workflows(self) -> Iterator[str]:
        """
        Retrieve all workflow types (workflow_task_ids) stored in this state backend.

        :return: Iterator of workflow task IDs representing different workflow types
        """
        return iter(self._workflow_types)

    def get_all_workflows_runs(self) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :return: Iterator of workflow identities for runs
        """
        return itertools.chain.from_iterable(self._workflow_runs.values())

    def get_workflow_runs(self, workflow_task_id: str) -> Iterator["WorkflowIdentity"]:
        """
        Retrieve workflow run identities from this state backend.

        :param workflow_task_id: Filter for specific workflow type
        :return: Iterator of workflow identities for runs
        """
        return iter(self._workflow_runs.get(workflow_task_id, []))

    def store_workflow_sub_invocation(
        self, parent_workflow_id: str, sub_invocation_id: str
    ) -> None:
        """
        Store a sub-invocation ID that runs inside a parent workflow.

        :param parent_workflow_id: The workflow ID that contains the sub-invocation
        :param sub_invocation_id: The invocation ID of the task/sub-workflow running inside
        """
        self._workflow_sub_invocations[parent_workflow_id].add(sub_invocation_id)

    def get_workflow_sub_invocations(self, workflow_id: str) -> Iterator[str]:
        """
        Retrieve all sub-invocation IDs that run inside a specific workflow.

        :param workflow_id: The workflow ID to get sub-invocations for
        :return: Iterator of invocation IDs that run inside the workflow
        """
        return iter(self._workflow_sub_invocations.get(workflow_id, set()))
