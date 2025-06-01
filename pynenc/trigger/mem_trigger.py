"""
In-memory implementation of the Pynenc trigger subsystem.

This module provides an implementation of the trigger system that stores
all its state in memory. It's suitable for development and testing purposes.
"""

import threading
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timedelta, timezone
from typing import TYPE_CHECKING, Optional

from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import ConditionContext, TriggerCondition, ValidCondition

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.trigger_definitions import TriggerDefinition
    from pynenc.trigger.types import ConditionId, TaskId


class MemTrigger(BaseTrigger):
    """
    In-memory implementation of the Pynenc trigger system.

    This implementation stores all data in memory and is suitable for
    single-process applications or testing.
    """

    def __init__(self, app: "Pynenc") -> None:
        """
        Initialize the memory-based trigger component.

        :param app: The Pynenc application instance
        """
        super().__init__(app)
        # Map of condition_id to condition object
        self._conditions: dict[str, TriggerCondition] = {}
        # Map of trigger_id to trigger definition
        self._triggers: dict[str, TriggerDefinition] = {}
        # Map of condition_id to list of trigger_ids that use this condition
        self._condition_triggers: dict[str, list[str]] = defaultdict(list)
        # Map of condition_id to valid condition
        self._valid_conditions: dict[str, ValidCondition] = {}
        # Map of task_id to set of condition_ids that are sourced from this task
        self._source_task_conditions: dict[str, set[str]] = defaultdict(set)
        # Map of condition_id to last execution time (for cron conditions)
        self._last_cron_executions: dict[str, datetime] = {}
        # Lock to ensure thread safety for cron execution tracking
        self._cron_lock = threading.RLock()
        # Map of claimed trigger executions (trigger_id:valid_condition_id) -> expiration timestamp
        self._execution_claims: dict[str, datetime] = {}
        self._claim_lock = threading.RLock()
        # Map of claimed trigger runs (trigger_run_id) -> expiration timestamp
        self._trigger_run_claims: dict[str, datetime] = {}
        self._trigger_run_lock = threading.RLock()

    def _register_condition(self, condition: TriggerCondition) -> None:
        """
        Register a condition in the in-memory system.

        :param condition: The condition to register
        """
        condition_id = condition.condition_id
        self._conditions[condition_id] = condition

    def get_condition(self, condition_id: str) -> Optional[TriggerCondition]:
        """
        Get a condition by its ID from the in-memory store.

        :param condition_id: ID of the condition to retrieve
        :return: The condition if found, None otherwise
        """
        return self._conditions.get(condition_id)

    def register_trigger(self, trigger: "TriggerDefinition") -> None:
        """
        Register a trigger definition in the in-memory system.

        :param trigger: The trigger definition to register
        """
        self._triggers[trigger.trigger_id] = trigger

        # Map each condition to this trigger
        for condition_id in trigger.condition_ids:
            self._condition_triggers[condition_id].append(trigger.trigger_id)

    def get_trigger(self, trigger_id: str) -> Optional["TriggerDefinition"]:
        """
        Get a trigger definition by ID from the in-memory store.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        return self._triggers.get(trigger_id)

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinition"]:
        """
        Get all triggers that depend on a specific condition from the in-memory store.

        :param condition_id: ID of the condition
        :return: List of trigger definitions using this condition
        """
        trigger_ids = self._condition_triggers.get(condition_id, [])
        return [self._triggers[tid] for tid in trigger_ids if tid in self._triggers]

    def record_valid_condition(self, valid_condition: ValidCondition) -> None:
        """
        Record that a condition has been satisfied with a specific context in memory.

        :param valid_condition: The valid condition to record
        """
        self._valid_conditions[valid_condition.valid_condition_id] = valid_condition

    def record_valid_conditions(self, valid_conditions: list[ValidCondition]) -> None:
        """
        Record that multiple conditions have been satisfied with their respective contexts in memory.

        :param valid_conditions: The list of valid conditions to record
        """
        for valid_condition in valid_conditions:
            self.record_valid_condition(valid_condition)

    def get_valid_conditions(self) -> dict[str, ValidCondition]:
        """
        Get all currently valid conditions and their contexts from memory.

        :return: Dictionary mapping condition IDs to their valid conditions
        """
        return self._valid_conditions.copy()

    def clear_valid_conditions(self, conditions: Iterable["ValidCondition"]) -> None:
        """
        Clear valid conditions after they have been processed from memory.

        :param conditions: List of valid conditions to clear
        """
        for condition in conditions:
            if condition.valid_condition_id in self._valid_conditions:
                del self._valid_conditions[condition.valid_condition_id]

    def _get_all_conditions(self) -> list[TriggerCondition]:
        """
        Get all registered conditions from memory.

        :return: List of all conditions
        """
        return list(self._conditions.values())

    def get_last_cron_execution(self, condition_id: "ConditionId") -> datetime | None:
        """
        Get the timestamp of the last execution of a cron condition from memory.

        :param condition_id: ID of the cron condition
        :return: Timestamp of last execution, or None if never executed
        """
        with self._cron_lock:
            return self._last_cron_executions.get(condition_id)

    def store_last_cron_execution(
        self,
        condition_id: "ConditionId",
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        """
        Store the timestamp of the last execution of a cron condition in memory.

        This implementation uses thread locking to ensure atomicity
        and prevent race conditions in multi-threaded environments.

        :param condition_id: ID of the cron condition
        :param execution_time: Timestamp of the execution
        :param expected_last_execution: Expected current value for optimistic locking
        :return: True if stored successfully, False if another process already updated it
        """
        with self._cron_lock:
            current = self._last_cron_executions.get(condition_id)

            # If we expect a specific last execution time and it doesn't match,
            # it means someone else updated it
            if (
                expected_last_execution is not None
                and current != expected_last_execution
            ):
                return False

            self._last_cron_executions[condition_id] = execution_time
            return True

    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: "ConditionId"
    ) -> None:
        """
        Register the association between a source task and the conditions it affects.

        For in-memory implementation, the relationship is already stored in
        self._source_task_conditions during register_source_task_condition.

        :param task_id: ID of the source task
        :param condition_id: ID of the condition sourced from the task
        """
        self._source_task_conditions[task_id].add(condition_id)

    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type[ConditionContext] | None = None
    ) -> list["TriggerCondition"]:
        """
        Get all conditions that are sourced from a specific task.

        These are conditions that monitor the task and might be satisfied by its status or results.

        :param task_id: ID of the source task
        :param context_type: Optional context type to filter conditions by
        :return: List of conditions monitoring this task
        """
        condition_ids = self._source_task_conditions.get(task_id, set())
        conditions = [
            self._conditions[condition_id]
            for condition_id in condition_ids
            if condition_id in self._conditions
        ]
        if context_type is not None:
            conditions = [
                cond for cond in conditions if cond.context_type == context_type
            ]
        return conditions

    def claim_trigger_execution(
        self, trigger_id: str, valid_condition_id: str, expiration_seconds: int = 60
    ) -> bool:
        """
        Atomically claim the right to execute a trigger for a specific valid condition.

        Uses in-memory locking to prevent race conditions in multi-threaded environments.

        :param trigger_id: ID of the trigger being executed
        :param valid_condition_id: ID of the valid condition being processed
        :param expiration_seconds: Number of seconds after which the claim expires
        :return: True if the claim was successful, False if another worker has claimed it
        """
        claim_key = f"{trigger_id}:{valid_condition_id}"
        now = datetime.now(timezone.utc)
        expiration = now + timedelta(seconds=expiration_seconds)

        with self._claim_lock:
            # Check if there's an existing claim that hasn't expired
            if claim_key in self._execution_claims:
                existing_expiration = self._execution_claims[claim_key]
                if existing_expiration > now:
                    # Claim exists and hasn't expired
                    return False

            # Set our claim with expiration time
            self._execution_claims[claim_key] = expiration
            return True

    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        """
        Atomically claim the right to execute a trigger run.

        Uses in-memory locking to prevent race conditions in multi-threaded environments.

        :param trigger_run_id: Unique ID for this trigger run
        :param expiration_seconds: Number of seconds after which the claim expires
        :return: True if the claim was successful, False if another worker has claimed it
        """
        now = datetime.now(timezone.utc)
        expiration = now + timedelta(seconds=expiration_seconds)

        with self._trigger_run_lock:
            # Check if there's an existing claim that hasn't expired
            if trigger_run_id in self._trigger_run_claims:
                existing_expiration = self._trigger_run_claims[trigger_run_id]
                if existing_expiration > now:
                    # Claim exists and hasn't expired
                    return False

            # Set our claim with expiration time
            self._trigger_run_claims[trigger_run_id] = expiration
            return True

    def clean_task_trigger_definitions(self, task_id: str) -> None:
        """Remove all trigger definitions for a specific task from memory."""
        # Find all triggers for this task
        task_triggers = [t for t in self._triggers.values() if t.task_id == task_id]

        # Collect condition IDs and remove triggers
        for trigger in task_triggers:
            # Remove from triggers dictionary
            if trigger.trigger_id in self._triggers:
                del self._triggers[trigger.trigger_id]
            # Remove from condition_triggers mappings
            for condition_id in trigger.condition_ids:
                if condition_id in self._condition_triggers:
                    # Remove this trigger from the condition's list
                    self._condition_triggers[condition_id] = [
                        t_id
                        for t_id in self._condition_triggers[condition_id]
                        if t_id != trigger.trigger_id
                    ]
                    # Clean up empty lists
                    if not self._condition_triggers[condition_id]:
                        del self._condition_triggers[condition_id]

    def purge(self) -> None:
        """
        Purge all data from the in-memory trigger system.

        This method clears all registered conditions, triggers, and valid conditions.
        """
        self._conditions.clear()
        self._triggers.clear()
        self._valid_conditions.clear()
        self._condition_triggers.clear()
        self._source_task_conditions.clear()
        self._last_cron_executions.clear()
        self._execution_claims.clear()
        self._trigger_run_claims.clear()
