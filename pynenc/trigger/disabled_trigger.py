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
    from pynenc.invocation.base_invocation import BaseInvocation
    from pynenc.invocation.status import InvocationStatus
    from pynenc.trigger.trigger_definitions import TriggerDefinition
    from pynenc.trigger.types import ConditionId, TaskId


class DisabledTrigger(BaseTrigger):
    """A no-op implementation of the trigger component that disables all triggering functionality."""

    def __init__(self, app: "Pynenc") -> None:
        """Initialize with app reference, but skip BaseTrigger initialization."""
        self.app = app

    def _register_condition(self, condition: "TriggerCondition") -> None:
        """No-op implementation for condition registration."""

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: "ConditionId"
    ) -> None:
        """No-op implementation for source task condition registration."""

    def get_condition(self, condition_id: str) -> "TriggerCondition | None":
        """Always returns None as conditions are disabled."""
        return None

    def register_trigger(self, trigger: "TriggerDefinition") -> None:
        """No-op implementation for trigger registration."""

    def get_trigger(self, trigger_id: str) -> "TriggerDefinition | None":
        """Always returns None as triggers are disabled."""
        return None

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinition"]:
        """Always returns empty list as triggers are disabled."""
        return []

    def get_conditions_sourced_from_task(
        self, task_id: str, context_type: type["ConditionContext"] | None = None
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
        invocations: list["DistributedInvocation"],
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
        self, task_id: str, arguments: dict[str, Any] | None = None
    ) -> "BaseInvocation":
        """Raises NotImplementedError as task execution via triggers is disabled."""
        raise NotImplementedError("Task execution via triggers is disabled.")

    def clean_task_trigger_definitions(self, task_id: str) -> None:
        """No-op implementation for cleaning task trigger definitions."""

    def purge(self) -> None:
        """No-op implementation for purging trigger data."""
