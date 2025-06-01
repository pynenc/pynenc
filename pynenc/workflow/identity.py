import json
from dataclasses import dataclass
from typing import TYPE_CHECKING, Optional

if TYPE_CHECKING:
    from pynenc.invocation.base_invocation import InvocationIdentity
    from pynenc.task import Task


@dataclass(frozen=True)
class WorkflowIdentity:
    """
    Immutable identity of a workflow execution.

    Workflows provide a structured execution context for tasks, enabling complex orchestration
    and dependency management. Each workflow has a unique identity captured by this class.

    :param workflow_task_id: The identifier of the task that defines this workflow
    :param workflow_invocation_id: The unique identifier for this specific workflow execution
    :param parent_workflow: The parent workflow if this is a subworkflow
    """

    workflow_task_id: str
    workflow_invocation_id: str
    parent_workflow: Optional["WorkflowIdentity"] = None

    @property
    def workflow_id(self) -> str:
        """Get the unique identifier for this workflow."""
        return self.workflow_invocation_id

    @classmethod
    def from_invocation(cls, invocation: "InvocationIdentity") -> "WorkflowIdentity":
        """
        Create a new workflow identity based on the invocation.

        :param invocation: The invocation that defines this workflow
        :param parent_workflow: Optional parent workflow if this is a subworkflow
        :return: A new workflow identity
        """
        task: Task = invocation.call.task
        parent_workflow = (
            invocation.parent_invocation.workflow
            if invocation.parent_invocation
            else None
        )
        if not task.conf.force_new_workflow and parent_workflow is not None:
            task.app.logger.debug(f"New {invocation=} on {parent_workflow}")
            return parent_workflow

        new_wf = cls(
            workflow_task_id=task.task_id,
            workflow_invocation_id=invocation.invocation_id,
            parent_workflow=parent_workflow,
        )

        sub = "sub-" if parent_workflow else ""
        task.app.logger.info(f"Creating a new {sub}workflow {new_wf}")
        return new_wf

    @property
    def is_subworkflow(self) -> bool:
        """Check if this workflow is a subworkflow."""
        return self.parent_workflow is not None

    def to_json(self) -> str:
        """Convert the workflow identity to a JSON-serializable dictionary."""
        parent_workflow_json = (
            self.parent_workflow.to_json() if self.parent_workflow else None
        )
        return json.dumps(
            {
                "workflow_task_id": self.workflow_task_id,
                "workflow_invocation_id": self.workflow_invocation_id,
                "parent_workflow": parent_workflow_json,
            }
        )

    @classmethod
    def from_json(cls, json_str: str) -> "WorkflowIdentity":
        """Create a WorkflowIdentity from a JSON string."""
        data = json.loads(json_str)
        parent_workflow = (
            cls.from_json(data["parent_workflow"]) if data["parent_workflow"] else None
        )
        return cls(
            workflow_task_id=data["workflow_task_id"],
            workflow_invocation_id=data["workflow_invocation_id"],
            parent_workflow=parent_workflow,
        )
