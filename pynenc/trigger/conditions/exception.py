"""
Exception-based trigger conditions for task execution.

This module defines conditions based on the exception raised by a task,
enabling triggers when a task fails with specific exception types.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar

from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import ArgumentFilter
from pynenc.trigger.conditions.base import TriggerCondition
from pynenc.trigger.conditions.status import StatusCondition, StatusContext

if TYPE_CHECKING:
    from ...app import Pynenc


@dataclass
class ExceptionContext(StatusContext):
    """
    Context for exception-based conditions.

    This context holds the data required to evaluate conditions based on
    exceptions raised by a task. It extends StatusContext to add exception information.
    """

    exception_type: str
    exception_message: str

    @property
    def context_id(self) -> str:
        return f"exception_{self.exception_type}"

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this exception context.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        data = super()._to_json(app)
        data["exception_type"] = self.exception_type
        data["exception_message"] = self.exception_message
        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ExceptionContext":
        """
        Create an ExceptionContext from parsed JSON data.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new ExceptionContext instance
        """
        status_context = StatusContext._from_json(data, app)
        return cls(
            task_id=status_context.task_id,
            call_id=status_context.call_id,
            invocation_id=status_context.invocation_id,
            arguments=status_context.arguments,
            status=status_context.status,
            exception_type=data["exception_type"],
            exception_message=data["exception_message"],
        )


class ExceptionCondition(TriggerCondition[ExceptionContext]):
    """
    Condition based on the exception raised by a task.

    Triggers a task when another task raises a specific exception type.
    This condition combines task status filtering (requiring FAILED) with exception type filtering.
    It can filter based on:
    1. The task call arguments (to match specific task invocations)
    2. The exception type raised by the task
    """

    context_type: ClassVar[type[ExceptionContext]] = ExceptionContext

    def __init__(
        self,
        task_id: str,
        arguments_filter: ArgumentFilter,
        exception_types: list[str],
    ) -> None:
        """
        Create an exception condition.

        :param task_id: ID of the task to monitor
        :param arguments_filter: Filter for task call arguments
        :param exception_types: List of exception type names to match
        """
        self.exception_types = exception_types

        # Create an internal StatusCondition to reuse logic
        # For ExceptionCondition, we only want to trigger on FAILED status
        self._status_condition = StatusCondition(
            task_id=task_id,
            statuses=[InvocationStatus.FAILED],
            arguments_filter=arguments_filter,
        )

    @property
    def task_id(self) -> str:
        """
        Get the task ID that this condition monitors.

        :return: Task ID string
        """
        return self._status_condition.task_id

    @property
    def arguments_filter(self) -> ArgumentFilter:
        """
        Get the arguments filter for this condition.

        :return: The arguments filter
        """
        return self._status_condition.arguments_filter

    @property
    def statuses(self) -> list[InvocationStatus]:
        """
        Get the statuses that this condition triggers on.

        For exception conditions, this is always [InvocationStatus.FAILED]

        :return: List of statuses
        """
        return self._status_condition.statuses

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this exception condition.

        :return: A string ID based on task ID, arguments, and exception types
        """
        # Get the status condition ID as a base
        base_id = self._status_condition.condition_id

        # Add exception types to ID
        exception_str = (
            "_".join(sorted(self.exception_types)) if self.exception_types else "any"
        )
        exception_id = f"_exception_{exception_str}"

        return f"{base_id}{exception_id}"

    def get_source_task_ids(self) -> set[str]:
        return self._status_condition.get_source_task_ids()

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        data = self._status_condition._to_json(app)
        data["exception_types"] = self.exception_types
        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ExceptionCondition":
        """
        Create an ExceptionCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new ExceptionCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        status_condition = StatusCondition._from_json(data, app)
        return cls(
            task_id=status_condition.task_id,
            arguments_filter=status_condition.arguments_filter,
            exception_types=data["exception_types"],
        )

    def _is_satisfied_by(self, context: ExceptionContext) -> bool:
        """
        Check if a task exception satisfies this condition.

        :param context: Exception context with exception data
        :return: True if the task status condition is satisfied and exception matches
        """
        if not self._status_condition._is_satisfied_by(context):
            return False
        if not self.exception_types:
            return True
        return context.exception_type in self.exception_types

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        :param task_id: ID of the task to check
        :return: True if this condition watches the specified task
        """
        return self._status_condition.affects_task(task_id)
