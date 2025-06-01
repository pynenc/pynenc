from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, TypeVar

from pynenc.workflow.deterministic import DeterministicExecutor

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task
    from pynenc.workflow.identity import WorkflowIdentity

S = TypeVar("S")


class WorkflowContext:
    """
    Provides workflow capabilities for tasks within Pynenc.

    This class exposes methods for workflow state management, determinism, and
    durability. It's accessible via the `wf` property on any Pynenc task.

    :param task: The task this helper is attached to
    """

    def __init__(self, task: Task):
        """Initialize the workflow helper with its associated task."""
        self.task = task
        self._deterministic: DeterministicExecutor | None = None

    @property
    def deterministic(self) -> DeterministicExecutor:
        """Get the deterministic executor for this workflow context."""
        if self._deterministic is None:
            self._deterministic = DeterministicExecutor(
                self.task.invocation.workflow, self.task.app
            )
        return self._deterministic

    @property
    def app(self) -> Pynenc:
        """Get the Pynenc app instance associated with this workflow."""
        return self.task.app

    @property
    def identity(self) -> WorkflowIdentity:
        """Get the workflow identity for the current execution."""
        return self.task.invocation.workflow

    def get_data(self, key: str, default: Any = None) -> Any:
        """
        Get a value from the workflow's data store.

        Any task within the workflow can read data.

        :param key: The data key to retrieve
        :param default: Default value if the key doesn't exist
        :return: The stored value or default
        """
        return self.app.state_backend.get_workflow_data(self.identity, key, default)

    def set_data(self, key: str, value: Any) -> None:
        """
        Set a value in the workflow's data store.

        Only the main workflow task can write workflow data.

        :param key: The data key to set
        :param value: The value to store
        :raises WorkflowAccessError: If a non-owner task attempts to modify data
        """
        self.app.state_backend.set_workflow_data(self.identity, key, value)

    # DETERMINISTIC OPERATION METHODS

    def random(self) -> float:
        """
        Generate a deterministic random number in the workflow context.

        Returns the same sequence of "random" numbers on workflow replay.

        :return: A random float between 0.0 and 1.0
        """
        return self.deterministic.random()

    def utc_now(self) -> datetime.datetime:
        """
        Get the current time deterministically in the workflow context.

        Returns deterministic timestamps that advance consistently on workflow replay.

        :return: Current datetime with UTC timezone
        """
        return self.deterministic.utc_now()

    def uuid(self) -> str:
        """
        Generate a deterministic UUID in the workflow context.

        Returns the same sequence of UUIDs on workflow replay.

        :return: UUID string
        """
        return self.deterministic.uuid()

    # CHILD WORKFLOW/TASK EXECUTION

    def execute_task(self, task: Task, *args: Any, **kwargs: Any) -> Any:
        """
        Execute a task as part of this workflow with deterministic replay.

        On workflow replay, returns the recorded result without re-execution.

        :param task: The task to execute
        :param args: Positional arguments for the task
        :param kwargs: Keyword arguments for the task
        :return: Task result
        """
        return self.deterministic.execute_task(task, *args, **kwargs)

    def register_task_run(self, caller: DistributedInvocation | None) -> None:
        """Register a task run in the workflow context."""
        if self.task.is_main_workflow_task():
            # We are starting or retrying the current workflow
            # modify/initialize state appropiately
            # store state
            ...
        else:
            # It it's not the main task, then is just another tasks
            # running within the same workflow
            # we just addid to the state, or register it wwomehere
            ...
