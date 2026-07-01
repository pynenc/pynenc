from __future__ import annotations

import datetime
from typing import TYPE_CHECKING, Any, NoReturn, TypeVar

from pynenc.workflow.workflow_exceptions import (
    DeterministicOperationScopeError,
    WorkflowMembershipError,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.task import Task
    from pynenc.workflow.workflow_deterministic import DeterministicExecutor
    from pynenc.workflow.workflow_identity import WorkflowIdentity

S = TypeVar("S")


class WorkflowContext:
    """
    Provides workflow capabilities for tasks within Pynenc.

    This class exposes shared workflow state for any task that is running inside
    a workflow. Root-only orchestration lives on ``WorkflowRootContext.root``.

    :param task: The task this helper is attached to
    """

    def __init__(self, task: Task):
        """Initialize the workflow helper with its associated task."""
        self.task = task

    @property
    def app(self) -> Pynenc:
        """Get the Pynenc app instance associated with this workflow."""
        return self.task.app

    @property
    def identity(self) -> WorkflowIdentity:
        """Get the workflow identity for the current execution."""
        invocation = self.task.invocation
        if invocation.workflow is None:
            raise WorkflowMembershipError(
                operation_name="workflow",
                task_id=self.task.task_id.key,
                invocation_id=str(invocation.invocation_id),
            )
        return invocation.workflow

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

        Any task within the workflow can write data.

        :param key: The data key to set
        :param value: The value to store
        """
        self.app.state_backend.set_workflow_data(self.identity, key, value)


class WorkflowRootOps:
    """
    Root-only workflow orchestration operations.

    The public ``wf.root`` boundary makes it explicit that these methods are
    only valid on the workflow-defining invocation. The deterministic replay
    cursor itself is stored on the active invocation object, not on the cached
    task-level workflow context.
    """

    def __init__(self, task: Task):
        self.task = task

    @property
    def _invocation(self) -> DistributedInvocation:
        from pynenc.invocation.dist_invocation import DistributedInvocation

        invocation = self.task.invocation
        if not isinstance(invocation, DistributedInvocation):
            self._raise_scope_error("workflow orchestration")
        return invocation

    def _raise_scope_error(self, operation_name: str) -> NoReturn:
        invocation = None
        workflow = None
        invocation_id = None
        is_workflow_defining = False
        try:
            invocation = self.task.invocation
            invocation_id = str(invocation.invocation_id)
            workflow = invocation.workflow
            is_workflow_defining = bool(invocation.task.is_workflow_root_task)
        except Exception:
            pass
        raise DeterministicOperationScopeError(
            operation_name=operation_name,
            task_id=self.task.task_id.key,
            invocation_id=invocation_id,
            workflow_id=str(workflow.workflow_id) if workflow else None,
            workflow_type=workflow.workflow_type.key if workflow else None,
            parent_workflow_id=str(workflow.parent_workflow_id)
            if workflow and workflow.parent_workflow_id
            else None,
            is_workflow_defining_invocation=is_workflow_defining,
        )

    def _ensure_root_workflow_operation_allowed(
        self, operation_name: str
    ) -> DistributedInvocation:
        invocation = self._invocation
        if (
            not invocation.task.is_workflow_root_task
            or invocation.task.task_id != self.task.task_id
        ):
            self._raise_scope_error(operation_name)
        return invocation

    def _deterministic(self, operation_name: str) -> DeterministicExecutor:
        invocation = self._ensure_root_workflow_operation_allowed(operation_name)
        return invocation._get_deterministic_runtime()

    def random(self) -> float:
        """
        Generate a deterministic random number in the workflow context.

        Returns the same sequence of "random" numbers on workflow replay.

        :return: A random float between 0.0 and 1.0
        """
        return self._deterministic("random").random()

    def utc_now(self) -> datetime.datetime:
        """
        Get the current time deterministically in the workflow context.

        Returns deterministic timestamps that advance consistently on workflow replay.

        :return: Current datetime with UTC timezone
        """
        return self._deterministic("utc_now").utc_now()

    def uuid(self) -> str:
        """
        Generate a deterministic UUID in the workflow context.

        Returns the same sequence of UUIDs on workflow replay.

        :return: UUID string
        """
        return self._deterministic("uuid").uuid()

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
        return self._deterministic("execute_task").execute_task(task, *args, **kwargs)


class WorkflowRootContext(WorkflowContext):
    """Workflow context for workflow-defining tasks."""

    def __init__(self, task: Task):
        super().__init__(task)
        self.root = WorkflowRootOps(task)
