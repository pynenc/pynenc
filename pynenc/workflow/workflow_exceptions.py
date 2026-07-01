from __future__ import annotations

from typing import Any

from pynenc.exceptions import PynencError


class WorkflowError(PynencError):
    """Base class for workflow-related exceptions."""


class WorkflowPauseError(WorkflowError):
    """
    Raised when a workflow is explicitly paused.

    This exception is handled by the workflow runner, which will pause execution
    and store the workflow state for later resumption.
    """

    def __init__(self, reason: str = ""):
        self.reason = reason
        super().__init__(f"Workflow paused: {reason}")


class WorkflowMembershipError(WorkflowError):
    """Raised when workflow data is accessed outside workflow membership."""

    def __init__(
        self,
        operation_name: str,
        task_id: str,
        invocation_id: str | None,
        message: str | None = None,
    ) -> None:
        self.operation_name = operation_name
        self.task_id = task_id
        self.invocation_id = invocation_id
        self.message = message or (
            f"Task {task_id} is not running inside a workflow; "
            f"cannot use workflow operation {operation_name!r}."
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return self.message

    def _to_json_dict(self) -> dict[str, Any]:
        return {
            "operation_name": self.operation_name,
            "task_id": self.task_id,
            "invocation_id": self.invocation_id,
            "message": self.message,
        }

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> WorkflowMembershipError:
        return cls(
            operation_name=json_dict["operation_name"],
            task_id=json_dict["task_id"],
            invocation_id=json_dict["invocation_id"],
            message=json_dict["message"],
        )


class DeterministicOperationScopeError(WorkflowError):
    """Raised when root-only workflow orchestration is used from the wrong task."""

    def __init__(
        self,
        operation_name: str,
        task_id: str,
        invocation_id: str | None,
        workflow_id: str | None,
        workflow_type: str | None,
        parent_workflow_id: str | None,
        is_workflow_defining_invocation: bool,
        message: str | None = None,
    ) -> None:
        self.operation_name = operation_name
        self.task_id = task_id
        self.invocation_id = invocation_id
        self.workflow_id = workflow_id
        self.workflow_type = workflow_type
        self.parent_workflow_id = parent_workflow_id
        self.is_workflow_defining_invocation = is_workflow_defining_invocation
        self.message = message or (
            f"Workflow orchestration operation {operation_name!r} can only be "
            "used from the workflow-defining task. Use @app.workflow and call "
            "the operation through task.wf.root from that workflow task."
        )
        super().__init__(self.message)

    def __str__(self) -> str:
        return (
            f"{self.message} "
            f"(task_id={self.task_id}, invocation_id={self.invocation_id}, "
            f"workflow_id={self.workflow_id}, workflow_type={self.workflow_type}, "
            f"parent_workflow_id={self.parent_workflow_id}, "
            f"is_workflow_defining_invocation={self.is_workflow_defining_invocation})"
        )

    def _to_json_dict(self) -> dict[str, Any]:
        return {
            "operation_name": self.operation_name,
            "task_id": self.task_id,
            "invocation_id": self.invocation_id,
            "workflow_id": self.workflow_id,
            "workflow_type": self.workflow_type,
            "parent_workflow_id": self.parent_workflow_id,
            "is_workflow_defining_invocation": self.is_workflow_defining_invocation,
            "message": self.message,
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, Any]
    ) -> DeterministicOperationScopeError:
        return cls(
            operation_name=json_dict["operation_name"],
            task_id=json_dict["task_id"],
            invocation_id=json_dict["invocation_id"],
            workflow_id=json_dict["workflow_id"],
            workflow_type=json_dict["workflow_type"],
            parent_workflow_id=json_dict["parent_workflow_id"],
            is_workflow_defining_invocation=json_dict[
                "is_workflow_defining_invocation"
            ],
            message=json_dict["message"],
        )
