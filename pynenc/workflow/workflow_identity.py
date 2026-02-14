from dataclasses import dataclass
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.identifiers.task_id import TaskId


@dataclass(frozen=True)
class WorkflowIdentity:
    """
    Immutable identity of a workflow execution.

    Workflows provide a structured execution context for tasks, enabling complex orchestration
    and dependency management. Each workflow has a unique identity captured by this class.

    :param workflow_id: The unique identifier for the workflow (derived from the defining invocation)
    :param workflow_type: The type of the workflow (derived from the defining task)
    :param parent_workflow_id: The unique identifier for the parent workflow, if any
    """

    workflow_id: "InvocationId"
    workflow_type: "TaskId"
    parent_workflow_id: "InvocationId | None" = None

    @property
    def is_subworkflow(self) -> bool:
        """Check if this workflow is a subworkflow."""
        return self.parent_workflow_id is not None

    @classmethod
    def new_workflow(
        cls, invocation_id: "InvocationId", task_id: "TaskId"
    ) -> "WorkflowIdentity":
        return cls(
            workflow_id=invocation_id,
            workflow_type=task_id,
        )

    @classmethod
    def new_subworkflow(
        cls,
        invocation_id: "InvocationId",
        task_id: "TaskId",
        parent_workflow_id: "InvocationId",
    ) -> "WorkflowIdentity":
        return cls(
            workflow_id=invocation_id,
            workflow_type=task_id,
            parent_workflow_id=parent_workflow_id,
        )
