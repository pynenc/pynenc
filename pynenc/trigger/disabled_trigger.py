"""
No-op implementation of trigger functionality.

This module provides a trigger component implementation that disables all triggering functionality.
Can be used when triggering capability is not needed or should be explicitly disabled.
"""

from collections.abc import Iterable
from datetime import datetime
from typing import TYPE_CHECKING, Any, Optional

from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import ConditionContext, TriggerCondition, ValidCondition

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation import DistributedInvocation
    from pynenc.invocation.base_invocation import BaseInvocation, InvocationId
    from pynenc.invocation.status import InvocationStatus
    from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
    from pynenc.task import Task, TaskId
    from pynenc.trigger.trigger_builder import TriggerBuilder
    from pynenc.trigger.types import ConditionId


class DisabledTrigger(BaseTrigger):
    """A no-op implementation of the trigger component that disables all triggering functionality."""

    def __init__(self, app: "Pynenc") -> None:
        """Initialize with app reference, but skip BaseTrigger initialization."""
        self.app = app

    def register_condition(self, condition: "TriggerCondition") -> None:
        """No-op implementation for condition registration with a warning."""
        self.app.logger.warning(
            f"Registering triggering condition {condition.condition_id} with DisabledTrigger. "
            "The triggers will not work without a triggering backend."
        )

    def register_task_triggers(
        self,
        task: "Task",
        triggers: "TriggerBuilder | list[TriggerBuilder] | None" = None,
    ) -> None:
        """No-op implementation for task trigger registration with a warning."""
        if triggers:
            self.app.logger.warning(
                f"Registering triggers for task {task.task_id} with DisabledTrigger. "
                "The triggers will not work without a triggering backend."
            )

    def _register_condition(self, condition: "TriggerCondition") -> None:
        """No-op implementation for condition registration."""

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: "ConditionId"
    ) -> None:
        """No-op implementation for source task condition registration."""

    def get_condition(self, condition_id: str) -> "TriggerCondition | None":
        """Always returns None as conditions are disabled."""
        return None

    def register_trigger(self, trigger: "TriggerDefinitionDTO") -> None:
        """No-op implementation for trigger registration."""

    def _get_trigger(self, trigger_id: str) -> "TriggerDefinitionDTO | None":
        """Always returns None as triggers are disabled."""
        return None

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinitionDTO"]:
        """Always returns empty list as triggers are disabled."""
        return []

    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type["ConditionContext"] | None = None
    ) -> list["TriggerCondition"]:
        """Always returns empty list as conditions are disabled."""
        return []

    def record_valid_condition(self, valid_condition: "ValidCondition") -> None:
        """No-op implementation for recording valid conditions."""

    def record_valid_conditions(self, valid_conditions: list["ValidCondition"]) -> None:
        """No-op implementation for recording multiple valid conditions."""

    def get_valid_conditions(self) -> dict[str, "ValidCondition"]:
        """Always returns empty dict as conditions are disabled."""
        return {}

    def clear_valid_conditions(self, conditions: Iterable["ValidCondition"]) -> None:
        """No-op implementation for clearing valid conditions."""

    def _get_all_conditions(self) -> list["TriggerCondition"]:
        """Always returns empty list as conditions are disabled."""
        return []

    def get_last_cron_execution(self, condition_id: "ConditionId") -> datetime | None:
        """Always returns None as cron executions are disabled."""
        return None

    def report_tasks_status(
        self,
        invocation_ids: list["InvocationId"],
        status: Optional["InvocationStatus"] = None,
    ) -> None:
        """No-op implementation for reporting task status."""

    def report_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """No-op implementation for reporting invocation results."""

    def report_invocation_failure(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """No-op implementation for reporting invocation failures."""

    def emit_event(self, event_code: str, payload: dict[str, Any]) -> str:
        """No-op implementation for emitting events."""
        return ""

    def store_last_cron_execution(
        self,
        condition_id: "ConditionId",
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        """No-op implementation for storing last cron execution, always returns True."""
        return True

    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        """Always returns False to prevent trigger execution."""
        return False

    def execute_task(
        self, task_id: "TaskId", arguments: dict[str, Any] | None = None
    ) -> "BaseInvocation":
        """Raises NotImplementedError as task execution via triggers is disabled."""
        raise NotImplementedError("Task execution via triggers is disabled.")

    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
        """No-op implementation for cleaning task trigger definitions."""

    def _purge(self) -> None:
        """No-op implementation for purging trigger data."""

    def reload_task_conditions(self) -> None:
        """No-op implementation for reloading task conditions."""
