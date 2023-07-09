"""
Global Pynenc exception and warning classes.
"""
from typing import Optional, TYPE_CHECKING

if TYPE_CHECKING:
    from .arguments import Arguments
    from .call import Call
    from .invocation import BaseInvocation
    from .task import Task


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
        call_arguments: "Arguments",
        message: Optional[str] = None,
    ) -> None:
        self.existing_invocation = existing_invocation
        self.call_arguments = call_arguments
        super().__init__(task, message)

    def __str__(self) -> str:
        diff = set(self.existing_invocation.arguments.kwargs) - set(
            self.call_arguments.kwargs
        )
        if self.message:
            return f"SingleInvocationWithDifferentArgumentsError({self.task.task_id}) {diff=}: {self.message}"
        else:
            return f"SingleInvocationWithDifferentArgumentsError({self.task.task_id}) {diff=}"


class InvocationError(Exception):
    """Base class for all Task related errors."""

    def __init__(
        self, invocation: "BaseInvocation", message: Optional[str] = None
    ) -> None:
        self.invocation = invocation
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"InvocationError({self.invocation.invocation_id}): {self.message}"
        else:
            return f"InvocationError({self.invocation.invocation_id})"


class StateBackendError(Exception):
    """Error raised when a task will not be routed."""


class InvocationNotFoundError(StateBackendError):
    """Error raised when the invocation is not present in the State Backend."""

    def __init__(self, invocation_id: str, message: Optional[str] = None) -> None:
        self.invocation_id = invocation_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"InvocationNotFoundError({self.invocation_id}): {self.message}"
        else:
            return f"InvocationNotFoundError({self.invocation_id})"


class RunnerNotExecutableError(Exception):
    """Raised when trying to execute a runner that is not meant to be executed."""


class CycleDetectedError(Exception):
    """Raised when a cycle is detected in the DependencyGraph"""

    def __init__(self, cycle: list["Call"]) -> None:
        self.cycle = cycle
        message = f"A cycle was detected: {self._format_cycle(cycle)}"
        super().__init__(message)

    @staticmethod
    def _format_cycle(cycle: list["Call"]) -> str:
        calls_repr = []
        for call in cycle:
            task = call.task
            func_repr = f"{task.func.__module__}.{task.func.__name__}"
            args_repr = ", ".join(f"{k}:{v}" for k, v in call.arguments.kwargs.items())
            calls_repr.append(f"{func_repr}({args_repr})")

        calls_repr.append(f"back to {calls_repr[0]}")  # Closing the cycle

        formatted_cycle = "\n".join(f"- {call}" for call in calls_repr)

        return f"Cycle detected:\n{formatted_cycle}"
