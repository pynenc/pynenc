"""
Global Pynenc exception and warning classes.
"""
import json
from typing import TYPE_CHECKING, Any, Optional

from .util.subclasses import get_all_subclasses

if TYPE_CHECKING:
    from .call import Call
    from .invocation import BaseInvocation


class PynencError(Exception):
    """Base class for all Pynenc related errors."""

    def _to_json_dict(self) -> dict[str, Any]:
        """Returns a json serializable dictionary"""
        return self.__dict__

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "PynencError":
        """Returns a new error from the serialized json compatible dictionary"""
        return cls(**json_dict)

    def to_json(self) -> str:
        """Returns a string with the serialized error"""
        return json.dumps(self._to_json_dict())

    @classmethod
    def from_json(cls, error_name: str, serialized: str) -> "PynencError":
        """Returns the child class from a serialized error"""
        for subcls in get_all_subclasses(cls):
            if subcls.__name__ == error_name:
                return subcls._from_json_dict(json_dict=json.loads(serialized))
        raise ValueError(f"Unknown error type: {error_name}")


class RetryError(PynencError):
    """Error raised when a task should be retried."""


class PendingInvocationLockError(PynencError):
    """Error raised when two processes try to set the same invocation as pending concurrently"""

    def __init__(self, invocation_id: str) -> None:
        self.invocation_id = invocation_id


class TaskError(PynencError):
    """Base class for all Task related errors."""

    def __init__(self, task_id: str, message: Optional[str] = None) -> None:
        self.task_id = task_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"{self.__class__.__name__}({self.task_id}): {self.message}"
        else:
            return f"{self.__class__.__name__}({self.task_id})"

    def _to_json_dict(self) -> dict[str, Any]:
        """Returns a string with the serialized error"""
        return {"task_id": self.task_id, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "TaskError":
        """Returns a new error from a serialized error"""
        return cls(json_dict["task_id"], json_dict["message"])


class InvalidTaskOptionsError(TaskError):
    """Error raised when the task options are invalid."""


class TaskRoutingError(TaskError):
    """Error raised when a task will not be routed."""


class InvocationConcurrencyWithDifferentArgumentsError(TaskRoutingError):
    """
    Error raised when there is a pending task with different arguments
    than the current task.
    """

    def __init__(
        self,
        task_id: str,
        existing_invocation_id: str,
        new_call_id: str,
        diff: str,
        message: Optional[str] = None,
    ) -> None:
        self.existing_invocation_id = existing_invocation_id
        self.new_call_id = new_call_id
        self.diff = diff
        super().__init__(task_id, message)

    @classmethod
    def from_call_mismatch(
        cls,
        existing_invocation: "BaseInvocation",
        new_call: "Call",
        message: Optional[str] = None,
    ) -> "InvocationConcurrencyWithDifferentArgumentsError":
        return cls(
            existing_invocation.task.task_id,
            existing_invocation.invocation_id,
            new_call.call_id,
            cls.format_difference(existing_invocation.call, new_call),
            message,
        )

    @staticmethod
    def format_difference(existing_call: "Call", new_call: "Call") -> str:
        existing = existing_call.arguments.kwargs
        new = new_call.arguments.kwargs
        common_keys = set(existing.keys()).intersection(new.keys())
        removed_keys = set(existing.keys()).difference(new.keys())
        added_keys = set(new.keys()).difference(existing.keys())

        lines = []

        lines.append("==============================")
        lines.append(f"Differences for {existing_call.task.task_id}:")
        lines.append("==============================")
        lines.append(f"  * Original: {existing}")
        lines.append(f"  * Updated: {new}")
        lines.append("------------------------------")
        lines.append("  * Changes: ")

        for key in common_keys:
            if existing[key] != new[key]:
                lines.append(f"    - {key}: {existing[key]} -> {new[key]}")

        for key in removed_keys:
            lines.append(f"    - {key}: Removed")

        for key in added_keys:
            lines.append(f"    - {key}: Added")

        lines.append("==============================")

        return "\n".join(lines)

    def __str__(self) -> str:
        if self.message:
            return f"InvocationConcurrencyWithDifferentArgumentsError({self.task_id}) {self.message}\n{self.diff}"
        return f"InvocationConcurrencyWithDifferentArgumentsError({self.task_id})\n{self.diff}"

    def _to_json_dict(self) -> dict[str, Any]:
        """Returns a string with the serialized error"""
        return {
            **super()._to_json_dict(),
            "existing_invocation_id": self.existing_invocation_id,
            "new_call_id": self.new_call_id,
            "diff": self.diff,
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, str]
    ) -> "InvocationConcurrencyWithDifferentArgumentsError":
        """Returns a new error from a serialized error"""
        return cls(
            json_dict["task_id"],
            json_dict["existing_invocation_id"],
            json_dict["new_call_id"],
            json_dict["diff"],
            json_dict["message"],
        )


class InvocationError(PynencError):
    """Base class for all Task related errors."""

    def __init__(self, invocation_id: str, message: Optional[str] = None) -> None:
        self.invocation_id = invocation_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"InvocationError({self.invocation_id}): {self.message}"
        else:
            return f"InvocationError({self.invocation_id})"

    def _to_json_dict(self) -> dict[str, Any]:
        return {"invocation_id": self.invocation_id, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "InvocationError":
        return cls(json_dict["invocation_id"], json_dict["message"])


class StateBackendError(PynencError):
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

    def _to_json_dict(self) -> dict[str, Any]:
        return {"invocation_id": self.invocation_id, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "InvocationNotFoundError":
        return cls(json_dict["invocation_id"], json_dict["message"])


class RunnerNotExecutableError(PynencError):
    """Raised when trying to execute a runner that is not meant to be executed."""


class CycleDetectedError(PynencError):
    """Raised when a cycle is detected in the DependencyGraph"""

    def __init__(self, call_ids: list[str], message: str) -> None:
        self.call_ids = call_ids
        self.message = message
        super().__init__(message)

    @classmethod
    def from_cycle(cls, cycle: list["Call"]) -> "CycleDetectedError":
        call_ids = [call.call_id for call in cycle]
        message = f"A cycle was detected: {cls._format_cycle(cycle)}"
        return cls(call_ids, message)

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

    def _to_json_dict(self) -> dict[str, Any]:
        return {"call_ids": self.call_ids, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "CycleDetectedError":
        return cls(json_dict["call_ids"], json_dict["message"])


class RunnerError(PynencError):
    """Base class for all Runner related errors."""


class ConfigError(PynencError):
    """Base class for all the config related errors"""


class ConfigMultiInheritanceError(ConfigError):
    """Error related with multiinheritance of config fields"""


class AlreadyInitializedError(PynencError):
    """Error raised when trying to change the class of a component after it was initialized"""
