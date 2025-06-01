"""
Base classes and interfaces for Pynenc's trigger system.

This module defines the core abstractions for time-based scheduling and event-driven task execution.
The trigger component enables declarative scheduling of tasks through cron expressions, events,
and task dependency chains.

Key components:
- BaseTrigger: Abstract base class for trigger implementations
- TriggerCondition: Base class for different trigger conditions
- TriggerDefinition: Configuration linking conditions to tasks
"""

from abc import ABC, abstractmethod
from collections import defaultdict
from collections.abc import Iterable
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pynenc.arguments import Arguments
from pynenc.conf.config_trigger import ConfigTrigger
from pynenc.trigger.conditions import (
    CompositeLogic,
    CronCondition,
    CronContext,
    EventCondition,
    EventContext,
    ExceptionContext,
    ResultContext,
    StatusContext,
    ValidCondition,
)
from pynenc.trigger.trigger_context import TriggerContext

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.base_invocation import BaseInvocation
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.invocation.status import InvocationStatus
    from pynenc.task import Task
    from pynenc.trigger.conditions import ConditionContext, TriggerCondition
    from pynenc.trigger.trigger_builder import TriggerBuilder
    from pynenc.trigger.trigger_definitions import TriggerDefinition
    from pynenc.trigger.types import ConditionId, TaskId


class BaseTrigger(ABC):
    """
    Base class for the Trigger component in Pynenc.

    The Trigger component manages time-based and event-driven task execution
    by evaluating conditions and triggering tasks when appropriate.
    """

    def __init__(self, app: "Pynenc") -> None:
        """
        Initialize the trigger component with the parent application.

        :param app: The Pynenc application instance
        """
        self.app = app
        self._running = False
        self._registered_conditions: dict[ConditionId, TriggerCondition] = {}
        # Map of task_id to set of condition_ids that are sourced from this task
        self._source_task_conditions: dict[TaskId, set[ConditionId]] = defaultdict(set)
        # Local cache of last cron execution times
        self._last_cron_execution_cache: dict[ConditionId, datetime] = {}

    @cached_property
    def conf(self) -> ConfigTrigger:
        """Access to trigger configuration parameters."""
        return ConfigTrigger(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @abstractmethod
    def _register_condition(self, condition: "TriggerCondition") -> None:
        """
        Register a condition in the system.

        This makes the condition available for trigger definitions.

        :param condition: The condition to register
        """

    def register_condition(self, condition: "TriggerCondition") -> None:
        """
        Register a condition in the system.

        This method is called to register a new condition that can be used
        in trigger definitions.

        :param condition: The condition to register
        """
        if condition.condition_id not in self._registered_conditions:
            self._register_condition(condition)
            self._registered_conditions[condition.condition_id] = condition
            for source_task_id in condition.get_source_task_ids():
                self.register_source_task_condition(
                    source_task_id, condition.condition_id
                )
        else:
            self.app.logger.debug(
                f"Condition {condition.condition_id} already registered, skipping."
            )

    @abstractmethod
    def _register_source_task_condition(
        self, task_id: "TaskId", condition_id: "ConditionId"
    ) -> None:
        """
        Register the conditions that are sourced from a task.

        This method is called when a new condition based on a source task is registered.

        :param task_id: ID of the source task
        :param condition_id: ID of the condition sourced from the task
        """

    def register_source_task_condition(
        self, task_id: "TaskId", condition_id: "ConditionId"
    ) -> None:
        """
        Register a condition that is sourced from a specific task.

        This builds the reverse mapping from tasks to the conditions they affect,
        used for efficient lookup when task status changes.

        :param task_id: ID of the source task
        :param condition_id: ID of the condition sourced from the task
        """
        if condition_id not in self._source_task_conditions[task_id]:
            self._source_task_conditions[task_id].add(condition_id)
            self._register_source_task_condition(task_id, condition_id)
        else:
            self.app.logger.debug(
                f"Condition {condition_id} already registered for source task {task_id}, skipping."
            )

    @abstractmethod
    def get_condition(self, condition_id: str) -> "TriggerCondition | None":
        """
        Get a condition by its ID.

        :param condition_id: ID of the condition to retrieve
        :return: The condition if found, None otherwise
        """
        pass

    @abstractmethod
    def register_trigger(self, trigger: "TriggerDefinition") -> None:
        """
        Register a trigger definition.

        This creates a new trigger that will activate when its conditions are met.

        :param trigger: The trigger definition to register
        """
        pass

    @abstractmethod
    def get_trigger(self, trigger_id: str) -> "TriggerDefinition | None":
        """
        Get a trigger definition by ID.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        pass

    @abstractmethod
    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinition"]:
        """
        Get all triggers that depend on a specific condition.

        :param condition_id: ID of the condition
        :return: List of trigger definitions using this condition
        """
        pass

    @abstractmethod
    def get_conditions_sourced_from_task(
        self, task_id: str, context_type: type["ConditionContext"] | None = None
    ) -> list["TriggerCondition"]:
        """
        Get all conditions that are sourced from a specific task.

        These are conditions that monitor the task and might be satisfied by its status or results.

        :param task_id: ID of the source task
        :param context_type: Optional context type to filter conditions by
        :return: List of conditions monitoring this task
        """

    def validate_and_record_condition(
        self, condition: "TriggerCondition", context: "ConditionContext"
    ) -> bool:
        """
        Validate a condition against a context and record it if valid.

        This method checks if the given condition is satisfied by the context,
        and if so, creates and records a valid condition.

        :param condition: The condition to validate
        :param context: The context to validate against
        :return: True if the condition was satisfied and recorded, False otherwise
        """
        return self.validate_and_record_conditions([(condition, context)])[0]

    def validate_and_record_conditions(
        self,
        conditions_and_contexts: list[tuple["TriggerCondition", "ConditionContext"]],
    ) -> list[bool]:
        """
        Validate multiple conditions against a context and record them if valid.

        This method checks each condition in the list against the context,
        and records those that are satisfied.

        :param conditions: List of conditions to validate
        :param context: The context to validate against
        :return: List of booleans indicating if each condition was satisfied
        """
        valid_conditions = []
        result = []
        for condition, context in conditions_and_contexts:
            result.append(condition.is_satisfied_by(context))
            if result[-1]:
                valid_conditions.append(ValidCondition(condition, context))
        self.record_valid_conditions(valid_conditions)
        return result

    @abstractmethod
    def record_valid_condition(self, valid_condition: "ValidCondition") -> None:
        """
        Record that a condition has been satisfied with a specific context.

        This adds the condition to the list of valid conditions that can trigger tasks.

        :param valid_condition: The valid condition to record
        """

    @abstractmethod
    def record_valid_conditions(self, valid_conditions: list["ValidCondition"]) -> None:
        """
        Record that multiple conditions have been satisfied with their respective contexts.

        This adds the conditions to the list of valid conditions that can trigger tasks.

        :param valid_conditions: The list of valid conditions to record
        """

    @abstractmethod
    def get_valid_conditions(self) -> dict[str, "ValidCondition"]:
        """
        Get all currently valid conditions and their contexts.

        :return: Dictionary mapping condition IDs to their contexts
        """

    @abstractmethod
    def clear_valid_conditions(self, conditions: Iterable["ValidCondition"]) -> None:
        """
        Clear valid conditions after they have been processed.

        :param conditions: List of valid conditions to clear
        """

    @abstractmethod
    def clean_task_trigger_definitions(self, task_id: str) -> None:
        """
        Remove all trigger definitions for a specific task.

        This method should clean up any persisted trigger definitions
        associated with the task.

        :param task_id: ID of the task to clean triggers for
        """
        pass

    def register_task_triggers(
        self,
        task: "Task",
        triggers: "TriggerBuilder | list[TriggerBuilder] | None" = None,
    ) -> None:
        """
        Register triggers for a task.

        This method processes trigger definitions created through a TriggerBuilder
        and registers them with the trigger system.

        :param task: The task to trigger
        :param triggers: A TriggerBuilder or list of TriggerBuilders that define
                         when the task should be triggered
        """
        self.clean_task_trigger_definitions(task.task_id)
        if not triggers:
            return
        builders = triggers if isinstance(triggers, list) else [triggers]

        for builder in builders:
            for condition in builder.conditions:
                self.register_condition(condition)
            trigger_def = builder.build(task.task_id)
            self.register_trigger(trigger_def)

    def report_tasks_status(
        self,
        invocations: list["DistributedInvocation"],
        status: Optional["InvocationStatus"] = None,
    ) -> None:
        """
        Report status changes for multiple tasks to the trigger system in batch.

        This method efficiently processes status changes for multiple invocations
        by batching context creation and condition validation.

        :param invocations: List of invocations reporting status changes
        :param status: The new status for all invocations
        """
        condition_context_pairs: list[tuple[TriggerCondition, ConditionContext]] = []
        for invocation in invocations:
            # Create a status context for each invocation
            context = StatusContext.from_invocation(invocation, status)
            conditions = self.get_conditions_sourced_from_task(
                context.task_id, StatusContext
            )
            for condition in conditions:
                condition_context_pairs.append((condition, context))

        if condition_context_pairs:
            self.validate_and_record_conditions(condition_context_pairs)

    def report_invocation_result(
        self, invocation: "DistributedInvocation", result: Any
    ) -> None:
        """
        Report a task result to the trigger system.

        This method checks if any conditions are watching for this task's result,
        evaluates them, and records valid conditions.

        :param task_id: ID of the task reporting a result
        :param result: The result produced by the task
        :param invocation_id: ID of the specific invocation
        """
        # Create the result context
        context = ResultContext(
            task_id=invocation.task.task_id,
            call_id=invocation.call.call_id,
            invocation_id=invocation.invocation_id,
            arguments=invocation.call.arguments,
            status=invocation.status,
            result=result,
        )

        # Get conditions affected by this task
        conditions = self.get_conditions_sourced_from_task(
            invocation.task.task_id, ResultContext
        )

        # Evaluate each condition with the context
        for condition in conditions:
            self.validate_and_record_condition(condition, context)

    def report_invocation_failure(
        self, invocation: "DistributedInvocation", exception: Exception
    ) -> None:
        """
        Report a task exception to the trigger system.

        This method checks if any conditions are watching for this task's result,
        evaluates them, and records valid conditions.

        :param task_id: ID of the task reporting a result
        :param exception: The exception raised by the task
        :param invocation_id: ID of the specific invocation
        """
        # Create the exception context
        context = ExceptionContext(
            task_id=invocation.task.task_id,
            call_id=invocation.call.call_id,
            invocation_id=invocation.invocation_id,
            arguments=invocation.call.arguments,
            status=invocation.status,
            exception_type=type(exception).__name__,
            exception_message=str(exception),
        )

        # Get conditions affected by this task
        conditions = self.get_conditions_sourced_from_task(
            invocation.task.task_id, ExceptionContext
        )

        # Evaluate each condition with the context
        for condition in conditions:
            self.validate_and_record_condition(condition, context)

    def emit_event(self, event_code: str, payload: dict[str, Any]) -> str:
        """
        Emit an event into the system to potentially trigger tasks.

        Creates an event context, evaluates event conditions, and records
        valid conditions for triggering tasks.

        :param event_code: Type of the event
        :param payload: Data associated with the event
        :return: ID of the created event
        """
        # Generate an event ID
        import uuid

        event_id = str(uuid.uuid4())

        # Create the event context
        context = EventContext(
            event_code=event_code, payload=payload, event_id=event_id
        )

        event_conditions = [
            c
            for c in self._get_all_conditions()
            if isinstance(c, EventCondition) and c.event_code == event_code
        ]

        # Evaluate each condition
        for condition in event_conditions:
            self.validate_and_record_condition(condition, context)

        return event_id

    @abstractmethod
    def _get_all_conditions(self) -> list["TriggerCondition"]:
        """
        Get all registered conditions.

        :return: List of all conditions
        """
        pass

    @abstractmethod
    def get_last_cron_execution(self, condition_id: "ConditionId") -> datetime | None:
        """
        Get the timestamp of the last execution of a cron condition from persistent storage.

        :param condition_id: ID of the cron condition
        :return: Timestamp of last execution, or None if never executed
        """
        pass

    @abstractmethod
    def store_last_cron_execution(
        self,
        condition_id: "ConditionId",
        execution_time: datetime,
        expected_last_execution: datetime | None = None,
    ) -> bool:
        """
        Store the timestamp of the last execution of a cron condition in persistent storage.

        This should be implemented as an atomic operation to prevent race conditions
        in distributed environments.

        :param condition_id: ID of the cron condition
        :param execution_time: Timestamp of the execution
        :param expected_last_execution: Expected current value for optimistic locking
        :return: True if stored successfully, False if another process already updated it
        """
        pass

    def _should_trigger_cron_condition(
        self, condition: CronCondition, current_time: datetime
    ) -> bool:
        """
        Determine if a cron condition should be triggered based on its schedule and last execution.

        This method uses both local cache and persistent storage to efficiently determine
        if a cron condition should be triggered in a distributed environment.

        :param condition: The cron condition to check
        :param current_time: Current time to check against
        :return: True if the condition should be triggered, False otherwise
        """
        condition_id = condition.condition_id

        # Check local cache first for efficiency
        cached_last_execution = self._last_cron_execution_cache.get(condition_id)
        if cached_last_execution:
            # Create a CronContext with the last execution time
            context = CronContext(
                timestamp=current_time, last_execution=cached_last_execution
            )

            # If the condition isn't satisfied with the cached last execution,
            # no need to check persistent storage
            if not condition.is_satisfied_by(context):
                return False

        # If we're here, either no cached time or it's time to check again
        # Get the definitive last execution time from storage
        storage_last_execution = self.get_last_cron_execution(condition_id)

        # Update our cache with the latest value from storage
        if storage_last_execution:
            self._last_cron_execution_cache[condition_id] = storage_last_execution

            # Check if the condition is satisfied with the accurate last execution time
            context = CronContext(
                timestamp=current_time, last_execution=storage_last_execution
            )
            if not condition.is_satisfied_by(context):
                return False

        # Try to atomically update the last execution time
        success = self.store_last_cron_execution(
            condition_id, current_time, expected_last_execution=storage_last_execution
        )

        if success:
            # Update local cache
            self._last_cron_execution_cache[condition_id] = current_time
            return True
        else:
            # Another process beat us to it
            self.app.logger.info(
                f"Cron condition {condition_id} was triggered by another process."
            )
            # Update our cache with the latest value
            fresh_last_execution = self.get_last_cron_execution(condition_id)
            if fresh_last_execution:
                self._last_cron_execution_cache[condition_id] = fresh_last_execution
            return False

    def check_time_based_triggers(self, current_time: datetime | None = None) -> None:
        """
        Check and record valid time-based conditions.

        This method evaluates cron conditions and ensures they are only triggered
        once per scheduled interval, even in distributed environments.

        :param current_time: Current time to check against, defaults to now
        """
        # Create time context
        now = current_time or datetime.now(timezone.utc)
        context = CronContext(timestamp=now)

        time_conditions = [
            c for c in self._get_all_conditions() if isinstance(c, CronCondition)
        ]

        # Evaluate each condition
        for condition in time_conditions:
            if isinstance(condition, CronCondition):
                if not self._should_trigger_cron_condition(condition, now):
                    continue

            if self.validate_and_record_condition(condition, context):
                self.app.logger.debug(
                    f"Recorded valid time condition: {condition.condition_id}"
                )

    @abstractmethod
    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        """
        Atomically claim the right to execute a trigger run.

        This prevents multiple workers from executing the same trigger multiple times
        for the same context.

        :param trigger_run_id: Unique ID for this trigger run
        :param expiration_seconds: Number of seconds after which the claim expires
        :return: True if the claim was successful, False if another worker has claimed it
        """
        pass

    def trigger_loop_iteration(self) -> None:
        """
        Process one iteration of the trigger loop.

        This method is called periodically by the runner to:
        1. Check time-based triggers
        2. Process valid conditions
        3. Execute triggered tasks

        It uses atomic claiming to ensure each valid condition only triggers
        a task exactly once across all workers.
        """
        self.check_time_based_triggers()
        # Check for validated conditions
        valid_conditions = self.get_valid_conditions()
        if not valid_conditions:
            return
        # Finds triggers affected by the valid conditions
        # Keeps a reference from the valid conditions to the affected triggers
        affected_triggers: dict[str, tuple[TriggerDefinition, TriggerContext]] = {}
        condition_to_pending_triggers: dict[str, set[str]] = defaultdict(set)
        for valid_condition in valid_conditions.values():
            triggers = self.get_triggers_for_condition(
                valid_condition.condition.condition_id
            )
            for trigger in triggers:
                if trigger.trigger_id not in affected_triggers:
                    affected_triggers[trigger.trigger_id] = (trigger, TriggerContext())
                affected_triggers[trigger.trigger_id][1].add_valid_condition(
                    valid_condition
                )
                condition_to_pending_triggers[valid_condition.valid_condition_id].add(
                    trigger.trigger_id
                )
        # Check if the trigger should be executed
        # Remove the ran triggers from the affected triggers reference
        for trigger, context in affected_triggers.values():
            if not trigger.should_trigger(context):
                continue
            for vc_id in context.valid_conditions.keys():
                condition_to_pending_triggers[vc_id].discard(trigger.trigger_id)
            trigger_run_ids = trigger.generate_trigger_run_ids(context)
            for run_id in trigger_run_ids:
                if self.claim_trigger_run(run_id):
                    args = trigger.get_arguments(context)
                    self.execute_task(trigger.task_id, args)
                    # For OR logic, continue processing other run IDs
                    # For AND logic, only one run ID is generated, so this has no effect
                    if trigger.logic == CompositeLogic.AND:
                        break
        # Clean up the valid conditions that are no longer needed
        # Because all the triggers that required already ran
        conditions_to_clean = [
            valid_conditions[c]
            for c in condition_to_pending_triggers
            if not condition_to_pending_triggers[c]
        ]
        self.clear_valid_conditions(conditions_to_clean)

    def execute_task(
        self, task_id: str, arguments: dict[str, Any] | None = None
    ) -> "BaseInvocation":
        """
        Execute a task with the given arguments.

        This method handles the actual execution of a task when its trigger conditions are met.

        :param task_id: ID of the task to execute
        :param arguments: Arguments to pass to the task, if any
        """
        # Get the task from the app
        task = self.app.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")
        invocation = task._call(Arguments(kwargs=arguments or {}))
        self.app.logger.info(f"Triggered task {task_id} with arguments {arguments}")
        return invocation

    @abstractmethod
    def purge(self) -> None:
        """
        Purges all the trigger data for the current self.app.app_id.
        ```{important}
            This should only be used for testing purposes.
        ```
        """
