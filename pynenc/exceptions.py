"""
Global Pynenc exception and warning classes.
"""
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .task import Task
    from .invocation import BaseInvocation
    from .types import Args


class TaskError(Exception):
    """Base class for all Task related errors."""

    def __init__(self, task: "Task", message: Optional[str] = None) -> None:
        self.task = task
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"TaskError({self.task.task_id}): {self.message}"
        else:
            return f"TaskError({self.task.task_id})"


class TaskRoutingError(TaskError):
    """Error raised when a task will not be routed."""


class SingleInvocationWithDifferentArgumentsError(TaskRoutingError):
    """
    Error raised when there is a pending task with different arguments
    than the current task.
    """

    def __init__(
        self,
        task: "Task",
        existing_invocation: "BaseInvocation",
        call_arguments: "Args",
        message: Optional[str] = None,
    ) -> None:
        self.existing_invocation = existing_invocation
        self.call_arguments = call_arguments
        super().__init__(task, message)

    def __str__(self) -> str:
        diff = set(self.existing_invocation.arguments) - set(self.call_arguments)
        if self.message:
            return f"SingleInvocationWithDifferentArgumentsError({self.task.task_id}) {diff=}: {self.message}"
        else:
            return f"SingleInvocationWithDifferentArgumentsError({self.task.task_id}) {diff=}"
