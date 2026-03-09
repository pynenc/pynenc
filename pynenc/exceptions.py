"""
Global Pynenc exception and warning classes.
"""

import json
from typing import TYPE_CHECKING, Any

from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.task_id import TaskId
from pynenc.util.subclasses import get_all_subclasses

if TYPE_CHECKING:
    from pynenc.call import Call
    from pynenc.invocation import (
        BaseInvocation,
        InvocationStatus,
        InvocationStatusRecord,
    )


class PynencError(Exception):
    """Base class for all Pynenc related errors."""

    def _to_json_dict(self) -> dict[str, Any]:
        """:return: a json serializable dictionary"""
        return self.__dict__

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "PynencError":
        """:return: a new error from the serialized json compatible dictionary"""
        return cls(**json_dict)

    def to_json(self) -> str:
        """:return: the serialized error"""
        return json.dumps(self._to_json_dict())

    @classmethod
    def from_json(cls, error_name: str, serialized: str) -> "PynencError":
        """:return: the child class from a serialized error"""
        for subcls in [cls] + get_all_subclasses(cls):
            if subcls.__name__ == error_name:
                return subcls._from_json_dict(json_dict=json.loads(serialized))
        raise ValueError(f"Unknown error type: {error_name}")


class RetryError(PynencError):
    """Error raised when a task should be retried."""


class ConcurrencyRetryError(RetryError):
    """Error raised when a task should be retried due to concurrency control."""


class SerializationError(PynencError):
    """Error raised when an argument cannot be serialized.

    Wraps the original serialization error with context about which argument
    failed and truncated value information to aid debugging.
    """

    def __init__(self, message: str) -> None:
        self.message = message
        super().__init__(message)

    def __str__(self) -> str:
        return self.message


class TaskError(PynencError):
    """Base class for all Task related errors."""

    def __init__(self, task_id: "TaskId", message: str | None = None) -> None:
        self.task_id = task_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"{self.__class__.__name__}({self.task_id}): {self.message}"
        else:
            return f"{self.__class__.__name__}({self.task_id})"

    def _to_json_dict(self) -> dict[str, Any]:
        """:return: the serialized error"""
        return {"task_id_key": self.task_id.key, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "TaskError":
        """:return: the serialized error"""
        return cls(TaskId.from_key(json_dict["task_id_key"]), json_dict["message"])


class InvalidTaskOptionsError(TaskError):
    """Error raised when the task options are invalid."""


class TaskRoutingError(TaskError):
    """Error raised when a task will not be routed."""


class TaskParallelProcessingError(TaskError):
    """Error parallelizing a task."""


class InvocationConcurrencyWithDifferentArgumentsError(TaskRoutingError):
    """
    Error raised when there is a task with different arguments
    than the current task.
    """

    def __init__(
        self,
        task_id: "TaskId",
        existing_invocation_id: str,
        new_call_id: "CallId",
        diff: str,
        message: str | None = None,
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
        message: str | None = None,
    ) -> "InvocationConcurrencyWithDifferentArgumentsError":
        return cls(
            existing_invocation.call.task.task_id,
            existing_invocation.invocation_id,
            new_call.call_id,
            cls.format_difference(existing_invocation.call, new_call),
            message,
        )

    @staticmethod
    def format_difference(existing_call: "Call", new_call: "Call") -> str:
        """Format argument differences in a clear, concise way."""
        existing_args = existing_call.arguments.kwargs
        new_args = new_call.arguments.kwargs

        existing_keys = set(existing_args.keys())
        new_keys = set(new_args.keys())
        common_keys = existing_keys & new_keys
        removed_keys = existing_keys - new_keys
        added_keys = new_keys - existing_keys

        lines = [f"Arguments differ for task {existing_call.task.task_id}:"]

        # Show changed values
        changed = []
        for key in sorted(common_keys):
            if existing_args[key] != new_args[key]:
                changed.append(f"  {key}: {existing_args[key]!r} -> {new_args[key]!r}")

        if changed:
            lines.append("Changed:")
            lines.extend(changed)

        # Show removed keys
        if removed_keys:
            lines.append("Removed:")
            for key in sorted(removed_keys):
                lines.append(f"  {key}: {existing_args[key]!r}")

        # Show added keys
        if added_keys:
            lines.append("Added:")
            for key in sorted(added_keys):
                lines.append(f"  {key}: {new_args[key]!r}")

        return "\n".join(lines)

    def __str__(self) -> str:
        if self.message:
            return f"InvocationConcurrencyWithDifferentArgumentsError({self.task_id}) {self.message}\n{self.diff}"
        return f"InvocationConcurrencyWithDifferentArgumentsError({self.task_id})\n{self.diff}"

    def _to_json_dict(self) -> dict[str, Any]:
        """:return: the serialized error"""
        return {
            **super()._to_json_dict(),
            "existing_invocation_id": self.existing_invocation_id,
            "new_call_id_key": self.new_call_id.key,
            "diff": self.diff,
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, str]
    ) -> "InvocationConcurrencyWithDifferentArgumentsError":
        """:return: a new error from a serialized error"""
        return cls(
            TaskId.from_key(json_dict["task_id_key"]),
            json_dict["existing_invocation_id"],
            CallId.from_key(json_dict["new_call_id_key"]),
            json_dict["diff"],
            json_dict["message"],
        )


class InvocationError(PynencError):
    """Base class for all Task related errors."""

    def __init__(self, invocation_id: str, message: str | None = None) -> None:
        self.invocation_id = invocation_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"InvocationError(invocation:{self.invocation_id}): {self.message}"
        else:
            return f"InvocationError(invocation:{self.invocation_id})"

    def _to_json_dict(self) -> dict[str, Any]:
        return {"invocation_id": self.invocation_id, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "InvocationError":
        return cls(json_dict["invocation_id"], json_dict["message"])


class StateBackendError(PynencError):
    """Error raised when a task will not be routed."""


class InvocationNotFoundError(StateBackendError):
    """Error raised when the invocation is not present in the State Backend."""

    def __init__(self, invocation_id: str, message: str | None = None) -> None:
        self.invocation_id = invocation_id
        self.message = message

    def __str__(self) -> str:
        if self.message:
            return f"InvocationNotFoundError(invocation:{self.invocation_id}): {self.message}"
        else:
            return f"InvocationNotFoundError({self.invocation_id})"

    def _to_json_dict(self) -> dict[str, Any]:
        return {"invocation_id": self.invocation_id, "message": self.message}

    @classmethod
    def _from_json_dict(cls, json_dict: dict[str, Any]) -> "InvocationNotFoundError":
        return cls(json_dict["invocation_id"], json_dict["message"])


class RunnerNotExecutableError(PynencError):
    """Raised when trying to execute a runner that is not meant to be executed."""


class RunnerError(PynencError):
    """Base class for all Runner related errors."""


class ConfigError(PynencError):
    """Base class for all the config related errors"""


class ConfigMultiInheritanceError(ConfigError):
    """Error related with multiinheritance of config fields"""


class AlreadyInitializedError(PynencError):
    """Error raised when trying to change the class of a component after it was initialized"""


class InvocationStatusError(PynencError):
    """
    Base class for all invocation status related errors.

    Use this to catch any error related to status transitions, ownership, or final status modifications.
    """


class InvocationStatusRaceConditionError(InvocationStatusError):
    """
    Raised when a race condition is detected in non-atomic status updates.

    This error occurs in non-atomic orchestrators when the actual status differs
    from the expected status after a write operation, indicating concurrent modification.
    """

    def __init__(
        self,
        invocation_id: str,
        previous_status_record: "InvocationStatusRecord",
        expected_status_record: "InvocationStatusRecord",
        actual_status_record: "InvocationStatusRecord",
    ) -> None:
        """
        Create a race condition error.

        :param str invocation_id: The invocation that experienced the race condition
        :param InvocationStatusRecord previous_status_record: Status before the attempted change
        :param InvocationStatusRecord expected_status_record: Status that was intended
        :param InvocationStatusRecord actual_status_record: Status that was actually set
        """
        self.invocation_id = invocation_id
        self.previous_status_record = previous_status_record
        self.expected_status_record = expected_status_record
        self.actual_status_record = actual_status_record

        super().__init__(
            f"Race condition detected for invocation {invocation_id}: "
            f"expected {expected_status_record.status} (owner: {expected_status_record.runner_id}), "
            f"but found {actual_status_record.status} (owner: {actual_status_record.runner_id})"
        )

    def __str__(self) -> str:
        return (
            f"InvocationStatusRaceConditionError("
            f"invocation:{self.invocation_id}, "
            f"previous:{self.previous_status_record.status}, "
            f"expected:{self.expected_status_record.status}, "
            f"actual:{self.actual_status_record.status})"
        )

    def _to_json_dict(self) -> dict[str, Any]:
        return {
            "invocation_id": self.invocation_id,
            "previous_status_record": self.previous_status_record.to_json(),
            "expected_status_record": self.expected_status_record.to_json(),
            "actual_status_record": self.actual_status_record.to_json(),
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, Any]
    ) -> "InvocationStatusRaceConditionError":
        from pynenc.invocation.status import InvocationStatusRecord

        return cls(
            invocation_id=json_dict["invocation_id"],
            previous_status_record=InvocationStatusRecord.from_json(
                json_dict["previous_status_record"]
            ),
            expected_status_record=InvocationStatusRecord.from_json(
                json_dict["expected_status_record"]
            ),
            actual_status_record=InvocationStatusRecord.from_json(
                json_dict["actual_status_record"]
            ),
        )


class InvocationStatusTransitionError(InvocationStatusError):
    """
    Raised when attempting an invalid invocation status transition.

    This error occurs when trying to change an invocation's status
    to a state that is not allowed by the state machine rules.
    """

    def __init__(
        self,
        from_status: "InvocationStatus | None",
        to_status: "InvocationStatus",
        allowed_statuses: frozenset["InvocationStatus"],
    ) -> None:
        """
        Create an invalid state transition error.

        :param InvocationStatus | None from_status: Current status
        :param InvocationStatus to_status: Attempted target status
        :param frozenset[InvocationStatus] allowed_statuses: Statuses that are allowed from current state
        """
        self.from_status = from_status
        self.to_status = to_status
        self.allowed_statuses = allowed_statuses

        allowed_str = (
            ", ".join(str(s) for s in sorted(allowed_statuses))
            if allowed_statuses
            else "none"
        )
        from_str = str(from_status) if from_status else "new invocation"

        super().__init__(
            f"Cannot transition from {from_str} to {to_status}. "
            f"Allowed transitions: {allowed_str}"
        )

    def __str__(self) -> str:
        allowed = ", ".join(f"status:{s.value}" for s in sorted(self.allowed_statuses))
        return (
            f"InvocationStatusTransitionError("
            f"from_status:{self.from_status}, "
            f"to_status:{self.to_status}, "
            f"allowed_statuses:[{allowed}])"
        )

    def _to_json_dict(self) -> dict[str, Any]:
        return {
            "from_status": self.from_status,
            "to_status": self.to_status,
            "allowed_statuses": list(self.allowed_statuses),
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, Any]
    ) -> "InvocationStatusTransitionError":
        return cls(
            json_dict["from_status"],
            json_dict["to_status"],
            frozenset(json_dict["allowed_statuses"]),
        )


class InvocationStatusOwnershipError(InvocationStatusError):
    """
    Raised when attempting to modify an invocation without proper ownership.

    This error occurs when:
    - A non-owner tries to modify an owned invocation
    - Attempting to acquire ownership without providing a runner ID
    - A status requiring ownership has no owner set (invalid state)
    """

    def __init__(
        self,
        from_status: "InvocationStatus | None",
        to_status: "InvocationStatus",
        current_owner: str | None,
        attempted_owner: str | None,
        reason: str,
    ) -> None:
        """
        Create an invocation ownership error.

        :param InvocationStatus | None from_status: Current status
        :param InvocationStatus to_status: Attempted target status
        :param str | None current_owner: Current owner ID
        :param str | None attempted_owner: ID of runner attempting the change
        :param str reason: Explanation of why ownership was violated
        """
        self.from_status = from_status
        self.to_status = to_status
        self.current_owner = current_owner
        self.attempted_owner = attempted_owner
        self.reason = reason

        super().__init__(
            f"Ownership violation transitioning from {from_status} to {to_status}: {reason}"
        )

    def __str__(self) -> str:
        return (
            f"InvocationStatusOwnershipError("
            f"from_status:{self.from_status}, "
            f"to_status:{self.to_status}, "
            f"current-owner-runner:{self.current_owner}, "
            f"attempted-owner-runner:{self.attempted_owner}, "
            f"reason:{self.reason})"
        )

    def _to_json_dict(self) -> dict[str, Any]:
        return {
            "from_status": self.from_status,
            "to_status": self.to_status,
            "current_owner": self.current_owner,
            "attempted_owner": self.attempted_owner,
            "reason": self.reason,
        }

    @classmethod
    def _from_json_dict(
        cls, json_dict: dict[str, Any]
    ) -> "InvocationStatusOwnershipError":
        return cls(
            json_dict["from_status"],
            json_dict["to_status"],
            json_dict["current_owner"],
            json_dict["attempted_owner"],
            json_dict["reason"],
        )
