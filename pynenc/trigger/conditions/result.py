"""
Result-based trigger conditions for task execution.

This module defines conditions based on the result of a task,
enabling triggers when a task produces specific result values.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, cast

from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import ArgumentFilter
from pynenc.trigger.arguments.result_filter import ResultFilterProtocol
from pynenc.trigger.conditions.base import TriggerCondition
from pynenc.trigger.conditions.status import StatusCondition, StatusContext

if TYPE_CHECKING:
    from ...app import Pynenc


@dataclass
class ResultContext(StatusContext):
    """
    Context for result-based conditions.

    This context holds the data required to evaluate conditions based on the result of a task.
    It extends StatusContext to add result information.
    """

    result: Any

    @property
    def context_id(self) -> str:
        return f"result_{self.invocation_id}"

    def __post_init__(self) -> None:
        """
        Validate the context after initialization.
        Ensures that the status is final
        """
        if not self.status == InvocationStatus.SUCCESS:
            raise ValueError("Status must be success")

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this result context.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        data = super()._to_json(app)

        # Serialize the result
        data["result"] = app.arg_cache.serialize(self.result)

        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ResultContext":
        """
        Create a ResultContext from parsed JSON data.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new ResultContext instance
        """
        # Get required fields
        status_context = StatusContext._from_json(data, app)
        result_data = data.get("result")

        # Deserialize the result if it was serialized
        result = None
        if result_data is not None:
            result = app.arg_cache.deserialize(result_data)

        # Create instance with proper fields
        return cls(
            task_id=status_context.task_id,
            call_id=status_context.call_id,
            invocation_id=status_context.invocation_id,
            arguments=status_context.arguments,
            status=status_context.status,
            result=result,
        )


class ResultCondition(TriggerCondition[ResultContext]):
    """
    Condition based on the result of a task.

    Triggers a task when another task produces a specific result.
    This can filter based on both:
    1. The task call arguments (to match specific task invocations)
    2. The result value (to trigger based on specific result values)

    This implementation composes with StatusCondition to reuse its functionality
    while maintaining proper typing.
    """

    context_type: ClassVar[type[ResultContext]] = ResultContext

    def __init__(
        self,
        task_id: str,
        arguments_filter: ArgumentFilter,
        result_filter: ResultFilterProtocol,
    ) -> None:
        """
        Create a result condition.

        :param task_id: ID of the task to monitor
        :param arguments_filter: Filter for task call arguments
        :param result_filter: Filter to apply to the result
        """
        # Store the result filter
        self.result_filter = result_filter

        # Create an internal StatusCondition to reuse logic
        # For ResultCondition, we only want to trigger on SUCCESS status
        self._status_condition = StatusCondition(
            task_id=task_id,
            statuses=[InvocationStatus.SUCCESS],
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

        For result conditions, this is always [InvocationStatus.SUCCESS]

        :return: List of statuses
        """
        return self._status_condition.statuses

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this result condition.

        :return: A string ID based on the task ID, arguments, and result filter
        """
        # Get the status condition ID as a base
        base_id = self._status_condition.condition_id
        return f"{base_id}_result_{self.result_filter.filter_id}"

    def get_source_task_ids(self) -> set[str]:
        return self._status_condition.get_source_task_ids()

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        # Start with the status condition data
        data = self._status_condition._to_json(app)
        data["result_filter"] = self.result_filter.to_json(app)

        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ResultCondition":
        """
        Create a ResultCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new ResultCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        status_condition = StatusCondition._from_json(data, app)
        result_filter = ArgumentFilter.from_json(data["result_filter"], app)
        return cls(
            task_id=status_condition.task_id,
            arguments_filter=status_condition.arguments_filter,
            result_filter=cast(ResultFilterProtocol, result_filter),
        )

    def _is_satisfied_by(self, context: ResultContext) -> bool:
        """
        Check if a task result satisfies this condition.

        :param context: Result context with result data
        :return: True if the task status condition is satisfied and result passes the filter
        """
        # First check if the status condition is satisfied
        # This will check task_id, status and arguments
        if not self._status_condition._is_satisfied_by(context):
            return False

        # Then check if the result matches the filter
        return self.result_filter.filter_result(context.result)

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        :param task_id: ID of the task to check
        :return: True if this condition watches the specified task
        """
        return self._status_condition.affects_task(task_id)
