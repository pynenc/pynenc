# Add these new exception classes


class WorkflowError(Exception):
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
