"""
Status-based trigger conditions for Pynenc.

This module provides conditions that trigger based on task and call status changes,
allowing tasks to be triggered when other tasks reach specific states.
"""

from dataclasses import dataclass
from typing import TYPE_CHECKING, Any, ClassVar, Optional

from pynenc.arguments import Arguments
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.arguments import ArgumentFilter
from pynenc.trigger.conditions.base import ConditionContext, TriggerCondition

if TYPE_CHECKING:
    from pynenc.invocation.dist_invocation import DistributedInvocation

    from ...app import Pynenc


@dataclass
class StatusContext(ConditionContext):
    """
    Context for task status conditions.

    Contains the task ID, call ID, invocation ID, status, and call arguments
    to evaluate status-based conditions.
    """

    task_id: str
    call_id: str
    invocation_id: str
    arguments: Arguments
    status: InvocationStatus

    @property
    def context_id(self) -> str:
        return f"status_{self.invocation_id}_{self.status}"

    @classmethod
    def from_invocation(
        cls,
        invocation: "DistributedInvocation",
        status: Optional[InvocationStatus] = None,
    ) -> "StatusContext":
        """
        Create a StatusContext from a DistInvocation.

        :param invocation: The invocation to extract context from
        :param status: Optional status to override the invocation's status
        :return: A StatusContext with task ID, call ID, invocation ID, status, and call arguments
        """
        return cls(
            task_id=invocation.task.task_id,
            call_id=invocation.call_id,
            invocation_id=invocation.invocation_id,
            status=status or invocation.status,
            arguments=invocation.call.arguments,
        )

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this status context.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        return {
            "task_id": self.task_id,
            "call_id": self.call_id,
            "invocation_id": self.invocation_id,
            "status": self.status.value,
            "arguments": self.arguments.to_json(app),
        }

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "StatusContext":
        """
        Create a StatusContext from parsed JSON data.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new StatusContext instance
        """
        # Get required fields
        task_id = data.get("task_id")
        call_id = data.get("call_id")
        invocation_id = data.get("invocation_id")
        status_str = data.get("status")
        arguments_json = data.get("arguments", {})

        if not all([task_id, call_id, invocation_id, status_str]):
            raise ValueError("Missing required fields in StatusContext data")
        if not task_id:
            raise ValueError("Missing task_id in StatusContext data")
        if not call_id:
            raise ValueError("Missing call_id in StatusContext data")
        if not invocation_id:
            raise ValueError("Missing invocation_id in StatusContext data")
        if not status_str:
            raise ValueError("Missing status in StatusContext data")
        status = InvocationStatus(status_str)

        return cls(
            task_id=task_id,
            call_id=call_id,
            invocation_id=invocation_id,
            status=status,
            arguments=Arguments.from_json(app, arguments_json),
        )


class StatusCondition(TriggerCondition[StatusContext]):
    """
    Condition based on task status changes.

    Triggers when a task reaches a specific status, with optional filtering by call arguments.
    """

    context_type: ClassVar[type[StatusContext]] = StatusContext

    def __init__(
        self,
        task_id: str,
        statuses: list[InvocationStatus],
        arguments_filter: ArgumentFilter,
    ):
        """
        Create a task status trigger condition.

        :param task_id: ID of the task to monitor
        :param statuses: Status(es) that satisfy this condition
        :param arguments_filter: Optional filter for task call arguments
        """
        self.task_id = task_id
        self.statuses = statuses
        self.arguments_filter = arguments_filter

    def get_source_task_ids(self) -> set[str]:
        return {self.task_id}

    @property
    def condition_id(self) -> str:
        """
        Generate a unique ID for this status condition.

        :return: A string ID based on task ID, statuses and call arguments
        """
        statuses_str = "_".join(sorted(self.statuses))
        base_id = (
            f"condition#{self.task_id}#{statuses_str}#{self.arguments_filter.filter_id}"
        )
        return base_id

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        data: dict = {
            "task_id": self.task_id,
            "statuses": self.statuses,
            "arguments_filter": self.arguments_filter.to_json(app),
        }
        return data

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "StatusCondition":
        """
        Create a StatusCondition from parsed JSON data.

        :param data: Dictionary with condition data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new StatusCondition instance
        :raises ValueError: If the data is invalid for this condition type
        """
        task_id = data.get("task_id")
        statuses = data.get("statuses", [])

        if not task_id:
            raise ValueError("Missing required task_id in StatusCondition data")
        if not statuses:
            raise ValueError("Missing required statuses in StatusCondition data")

        arguments_filter = ArgumentFilter.from_json(data["arguments_filter"], app)
        statuses = [InvocationStatus(status) for status in statuses]

        return cls(
            task_id=task_id, statuses=statuses, arguments_filter=arguments_filter
        )

    def _is_satisfied_by(self, context: StatusContext) -> bool:
        """
        Check if a task status change satisfies this condition.

        A status change satisfies the condition when:
        1. The task ID matches the condition's monitored task
        2. The status is in the list of monitored statuses
        3. The arguments filter matches the call arguments

        :param context: Status context with task ID, status, and call arguments
        :return: True if the condition is satisfied
        """
        if context.task_id != self.task_id:
            return False
        if context.status not in self.statuses:
            return False
        return self.arguments_filter.filter_arguments(context.arguments.kwargs)

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        :param task_id: ID of the task to check
        :return: True if this condition watches the specified task
        """
        return self.task_id == task_id
