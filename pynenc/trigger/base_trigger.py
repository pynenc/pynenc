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
from collections.abc import Iterable, Sequence
from datetime import UTC, datetime, timedelta
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pynenc.arguments import Arguments
from pynenc.conf.config_trigger import ConfigTrigger
from pynenc.conf.validation_trigger import validate_trigger_config
from pynenc import context as ctx_mod
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
from pynenc.trigger.log_messages import (
    TriggerLogMsg,
    context_extra_tokens,
    context_source_ref,
    join_tokens,
    ref,
    ref_list,
)
from pynenc.trigger.trigger_definitions import TriggerDefinition
from pynenc.trigger.trigger_context import TriggerContext

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.base_invocation import BaseInvocation, InvocationId
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.invocation.status import InvocationStatus
    from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
    from pynenc.orchestrator.atomic_service import AtomicServiceRun
    from pynenc.runner.runner_context import RunnerContext
    from pynenc.task import Task, TaskId
    from pynenc.trigger.conditions import ConditionContext, TriggerCondition
    from pynenc.trigger.monitoring import (
        EventMarkerPage,
        EventRecord,
        TriggerRunParticipant,
        TriggerRunRecord,
    )
    from pynenc.trigger.trigger_builder import TriggerBuilder
    from pynenc.trigger.types import ConditionId


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
        config = ConfigTrigger(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )
        validate_trigger_config(config)
        return config

    # ------------------------------------------------------------------ #
    # Registration and task-trigger wiring
    # ------------------------------------------------------------------ #
    # Public registration methods keep the in-process indexes in sync while
    # delegating durable storage to backend hooks implemented by Mem/SQLite.

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
                f"condition:{condition.condition_id} already registered, skipping."
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
                f"condition:{condition_id} already registered for task:{task_id}, skipping."
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
    def register_trigger(self, trigger: "TriggerDefinitionDTO") -> None:
        """
        Register a trigger definition.

        This creates a new trigger that will activate when its conditions are met.

        :param trigger: The trigger definition to register
        """
        pass

    def get_trigger(self, trigger_id: str) -> "TriggerDefinition | None":
        """
        Get a trigger definition by ID.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        if trigger_dto := self._get_trigger(trigger_id):
            return TriggerDefinition.from_dto(trigger_dto, self.app)
        return None

    @abstractmethod
    def _get_trigger(self, trigger_id: str) -> "TriggerDefinitionDTO | None":
        """
        Get a trigger definition by ID.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        pass

    @abstractmethod
    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinitionDTO"]:
        """
        Get all triggers that depend on a specific condition.

        :param condition_id: ID of the condition
        :return: List of trigger definitions using this condition
        """
        pass

    @abstractmethod
    def get_conditions_sourced_from_task(
        self, task_id: "TaskId", context_type: type["ConditionContext"] | None = None
    ) -> list["TriggerCondition"]:
        """
        Get all conditions that are sourced from a specific task.

        These are conditions that monitor the task and might be satisfied by its status or results.

        :param task_id: ID of the source task
        :param context_type: Optional context type to filter conditions by
        :return: List of conditions monitoring this task
        """

    # ------------------------------------------------------------------ #
    # Condition evaluation and valid-condition storage
    # ------------------------------------------------------------------ #
    # Evaluation is backend-independent; only persistence of satisfied
    # conditions is abstract so each backend can coordinate claims safely.

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

        Returns a list of booleans matching ``conditions_and_contexts``. Use
        :meth:`evaluate_and_record_conditions` when the caller needs the
        ``ValidCondition`` objects (for example to populate event monitoring).

        :param conditions_and_contexts: Pairs of condition and context.
        :return: List of booleans indicating if each condition was satisfied.
        """
        return [
            vc is not None
            for vc in self.evaluate_and_record_conditions(conditions_and_contexts)
        ]

    def evaluate_and_record_conditions(
        self,
        conditions_and_contexts: list[tuple["TriggerCondition", "ConditionContext"]],
    ) -> list["ValidCondition | None"]:
        """
        Evaluate conditions and record the satisfied ones.

        Returns a list of the same length as ``conditions_and_contexts``;
        each entry is either the recorded :class:`ValidCondition` or ``None``
        when the condition was not satisfied.

        :param conditions_and_contexts: Pairs of condition and context.
        :return: ``ValidCondition`` per pair, ``None`` for unmatched ones.
        """
        results: list[ValidCondition | None] = []
        to_record: list[ValidCondition] = []
        for condition, context in conditions_and_contexts:
            if condition.is_satisfied_by(context):
                vc = ValidCondition(condition, context)
                results.append(vc)
                to_record.append(vc)
                self._log_condition_matched(condition, context, vc)
            else:
                results.append(None)
        if to_record:
            self.record_valid_conditions(to_record)
        return results

    def _log_condition_matched(
        self,
        condition: "TriggerCondition",
        context: "ConditionContext",
        valid_condition: "ValidCondition",
    ) -> None:
        """Emit the predefined ``trigger.condition.matched`` log.

        Uses ``log_messages.context_source_ref`` so the source ref is
        consistent with the contract documented in
        ``docs/sprints/0004_1_triggers_workflows/11_pynmon_events_refinements.md``.
        """
        self.app.logger.info(
            join_tokens(
                TriggerLogMsg.CONDITION_MATCHED,
                ref("condition", condition.condition_id),
                ref("valid-condition", valid_condition.valid_condition_id),
                f"context:{type(context).__name__}",
                context_source_ref(context),
                *context_extra_tokens(context),
            )
        )

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
    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
        """
        Remove all trigger definitions for a specific task.

        This method should clean up any persisted trigger definitions
        associated with the task.

        :param task_id: ID of the task to clean triggers for
        """
        pass

    # ------------------------------------------------------------------ #
    # Task lifecycle reports
    # ------------------------------------------------------------------ #
    # Runners call these hooks when source tasks change state, produce a
    # result, or fail. The hooks translate runtime facts into condition
    # contexts that the trigger loop can process later.

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
            self.register_trigger(trigger_def.to_dto(self.app))

    def report_tasks_status(
        self,
        invocation_ids: list["InvocationId"],
        status: "InvocationStatus | None" = None,
    ) -> None:
        """
        Report status changes for multiple tasks to the trigger system in batch.

        This method efficiently processes status changes for multiple invocations
        by batching context creation and condition validation.

        :param invocation_ids: List of invocation IDs reporting status changes
        :param status: The new status for all invocations
        """
        condition_context_pairs: list[tuple[TriggerCondition, ConditionContext]] = []
        for invocation_id in invocation_ids:
            # Create a status context for each invocation
            invocation = self.app.state_backend.get_invocation(invocation_id)
            context = StatusContext.from_invocation(invocation, status)
            conditions = self.get_conditions_sourced_from_task(
                context.call_id.task_id, StatusContext
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
            call_id=invocation.call.call_id,
            invocation_id=invocation.invocation_id,
            arguments=invocation.call.arguments,
            disable_cache_args=invocation.call.task.conf.disable_cache_args,
            # get status directly from orchestrator to avoid caching
            status=self.app.orchestrator.get_invocation_status(
                invocation.invocation_id
            ),
            result=result,
        )

        # Get conditions affected by this task
        conditions = self.get_conditions_sourced_from_task(
            context.call_id.task_id, ResultContext
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
            call_id=invocation.call.call_id,
            invocation_id=invocation.invocation_id,
            arguments=invocation.call.arguments,
            disable_cache_args=invocation.call.task.conf.disable_cache_args,
            status=invocation.status,
            exception_type=type(exception).__name__,
            exception_message=str(exception),
        )

        # Get conditions affected by this task
        conditions = self.get_conditions_sourced_from_task(
            invocation.call.task.task_id, ExceptionContext
        )

        # Evaluate each condition with the context
        for condition in conditions:
            self.validate_and_record_condition(condition, context)

    # ------------------------------------------------------------------ #
    # Event emission
    # ------------------------------------------------------------------ #
    # Events are both trigger inputs and monitoring records. Storage failures
    # are logged but never allowed to break the emitting task.

    def emit_event(self, event_code: str, payload: dict[str, Any]) -> str:
        """
        Emit an event into the system to potentially trigger tasks.

        Creates an event context, evaluates event conditions, and records
        valid conditions for triggering tasks. The event is also persisted
        as an :class:`EventRecord` for monitoring even when no condition
        matches.

        :param event_code: Type of the event
        :param payload: Data associated with the event
        :return: ID of the created event
        """
        import uuid

        event_id = str(uuid.uuid4())
        context = EventContext(
            event_code=event_code, payload=payload, event_id=event_id
        )
        # Share a single timezone-aware UTC timestamp with the EventContext so
        # the monitoring record and the condition evaluation use the same time.
        timestamp = context.timestamp

        runner_context = ctx_mod.get_or_create_runner_context(self.app.app_id)
        record = self._build_event_record(
            event_id,
            event_code,
            payload,
            timestamp,
            runner_context.runner_id,
        )
        self._safe_store_event(
            record,
            runner_context,
            "trigger.store_event failed for event %s",
        )

        self.app.logger.info(
            join_tokens(
                TriggerLogMsg.EVENT_EMITTED,
                ref("event", event_id),
                f"code:{event_code}",
                ref("invocation", record.emitted_by_invocation_id),
                f"task:{record.emitted_by_task_id}"
                if record.emitted_by_task_id
                else "",
            )
        )

        event_conditions = self._event_conditions_for(event_code)
        valid = self.evaluate_and_record_conditions(
            [(c, context) for c in event_conditions]
        )
        matched_ids = [
            c.condition_id for c, vc in zip(event_conditions, valid, strict=False) if vc
        ]
        valid_ids = [vc.valid_condition_id for vc in valid if vc]
        if matched_ids:
            record.matched_condition_ids = matched_ids
            record.valid_condition_ids = valid_ids
            self._safe_store_event(
                record,
                runner_context,
                "trigger.store_event update failed for event %s",
            )
        return event_id

    def _build_event_record(
        self,
        event_id: str,
        event_code: str,
        payload: dict[str, Any],
        timestamp: datetime,
        runner_context_id: str,
    ) -> "EventRecord":
        """Build a fresh :class:`EventRecord` for the current emitter context."""
        from pynenc.trigger.monitoring import EventRecord

        emitter_inv_id, emitter_task_id = self._current_emitter_context()
        return EventRecord(
            event_id=event_id,
            event_code=event_code,
            payload=payload,
            timestamp=timestamp,
            emitted_by_invocation_id=emitter_inv_id,
            emitted_by_task_id=emitter_task_id,
            emitted_by_runner_context_id=runner_context_id,
        )

    def _safe_store_event(
        self,
        record: "EventRecord",
        runner_context: "RunnerContext",
        log_template: str,
    ) -> None:
        """Persist ``record`` swallowing storage failures per the monitoring policy.

        Monitoring writes must never break the hot path. This helper centralizes
        the "log and continue" policy documented for the trigger component.
        """
        try:
            self.app.state_backend.store_runner_context(runner_context)
            self.store_event(record)
        except Exception:  # pragma: no cover - logged, never re-raised
            self.app.logger.exception(log_template, record.event_id)

    def _event_conditions_for(self, event_code: str) -> list["EventCondition"]:
        """Return registered :class:`EventCondition` objects for ``event_code``."""
        return [
            c
            for c in self._get_all_conditions()
            if isinstance(c, EventCondition) and c.event_code == event_code
        ]

    def _current_emitter_invocation_id(self) -> str | None:
        """Return the invocation ID currently emitting an event, if any.

        Reads ``pynenc.context.get_dist_invocation_context`` (thread-local)
        which is populated by ``DistributedInvocation.run`` for the duration
        of a task body. Returns ``None`` outside any task.
        """
        return self._current_emitter_context()[0]

    def _current_emitter_context(self) -> tuple[str | None, str | None]:
        """Return ``(invocation_id, task_id_key)`` for the emitting task, if any."""
        from pynenc import context

        invocation = context.get_dist_invocation_context(self.app.app_id)
        if invocation is None:
            return None, None
        return str(invocation.invocation_id), str(invocation.call.task.task_id.key)

    # ------------------------------------------------------------------ #
    # Cron scheduling state
    # ------------------------------------------------------------------ #
    # Cron conditions use a local cache for fast rejection and backend storage
    # for the distributed claim that prevents duplicate scheduled runs.

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

        :param str condition_id: ID of the cron condition
        :return: Timestamp of last execution in UTC timezone, or None if never executed
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

        :param str condition_id: ID of the cron condition
        :param datetime execution_time: Timestamp of the execution (must be UTC timezone-aware)
        :param datetime | None expected_last_execution: Expected current value for optimistic locking (UTC timezone-aware)
        :return: True if stored successfully, False if another process already updated it
        """
        pass

    def _should_trigger_cron_condition(
        self,
        condition: CronCondition,
        current_time: datetime,
        atomic_service_run: "AtomicServiceRun",
    ) -> CronContext | None:
        """
        Determine if a cron condition should be triggered based on its schedule and last execution.

        This method uses both local cache and persistent storage to efficiently determine
        if a cron condition should be triggered in a distributed environment. All datetime
        values are expected to be UTC timezone-aware.

        :param CronCondition condition: The cron condition to check
        :param datetime current_time: Current time to check against (UTC timezone-aware)
        :param AtomicServiceRun atomic_service_run: Atomic-service run evaluating cron.
        :return: CronContext if the condition is valid and should be triggered, None otherwise
        """
        condition_id = condition.condition_id
        context = CronContext(timestamp=current_time)

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
                return None

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
                return None

        # Try to atomically update the last execution time
        success = self.store_last_cron_execution(
            condition_id, current_time, expected_last_execution=storage_last_execution
        )

        if success:
            # Update local cache
            self._last_cron_execution_cache[condition_id] = current_time
            self.app.logger.info(
                join_tokens(
                    TriggerLogMsg.CRON_CLAIMED,
                    ref("atomic-service-run", atomic_service_run.atomic_service_run_id),
                    ref("condition", condition_id),
                    f"timestamp:{current_time.isoformat()}",
                )
            )
            return context
        else:
            # Another process beat us to it
            self.app.logger.info(
                join_tokens(
                    TriggerLogMsg.CRON_SKIPPED,
                    ref("atomic-service-run", atomic_service_run.atomic_service_run_id),
                    ref("condition", condition_id),
                    "reason:already-claimed",
                )
            )
            # Update our cache with the latest value
            fresh_last_execution = self.get_last_cron_execution(condition_id)
            if fresh_last_execution:
                self._last_cron_execution_cache[condition_id] = fresh_last_execution
            return None

    def check_time_based_triggers(
        self,
        current_time: datetime | None = None,
        *,
        atomic_service_run: "AtomicServiceRun",
    ) -> None:
        """Check and record valid time-based conditions."""
        if not self.conf.enable_scheduler:
            return
        now = current_time or datetime.now(UTC)
        for condition in self._get_all_conditions():
            if not isinstance(condition, CronCondition):
                continue
            if triggering_context := self._should_trigger_cron_condition(
                condition, now, atomic_service_run
            ):
                self.record_valid_conditions(
                    [ValidCondition(condition, triggering_context)]
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

    # ------------------------------------------------------------------ #
    # Trigger loop and task execution
    # ------------------------------------------------------------------ #
    # One loop iteration claims runnable trigger contexts, routes task calls,
    # and records monitoring data for the resulting trigger runs.

    def trigger_loop_iteration(self, atomic_service_run: "AtomicServiceRun") -> None:
        """Process one iteration of the trigger loop."""
        try:
            self._process_loop_iteration(atomic_service_run)
        finally:
            try:
                self.auto_purge_events()
            except Exception:  # pragma: no cover - logged, never re-raised
                self.app.logger.exception("trigger.auto_purge_events failed")

    def _process_loop_iteration(self, atomic_service_run: "AtomicServiceRun") -> None:
        """Body of ``trigger_loop_iteration`` extracted for purge wrapping."""
        self._process_loop_iteration_body(atomic_service_run)

    def _process_loop_iteration_body(
        self, atomic_service_run: "AtomicServiceRun"
    ) -> None:
        """Inner body of one trigger-loop iteration."""
        self.check_time_based_triggers(atomic_service_run=atomic_service_run)
        valid_conditions = self.get_valid_conditions()
        if not valid_conditions:
            return
        # Finds triggers affected by the valid conditions
        # Keeps a reference from the valid conditions to the affected triggers
        affected_triggers: dict[str, tuple[TriggerDefinition, TriggerContext]] = {}
        condition_to_pending_triggers: dict[str, set[str]] = defaultdict(set)
        for valid_condition in valid_conditions.values():
            trigger_dtos = self.get_triggers_for_condition(
                valid_condition.condition.condition_id
            )
            for trigger_dto in trigger_dtos:
                trigger = TriggerDefinition.from_dto(trigger_dto, self.app)
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
        for trigger, ctx in affected_triggers.values():
            for execution_ctx in self._execution_contexts_for_trigger(trigger, ctx):
                for vc_id in execution_ctx.valid_conditions.keys():
                    condition_to_pending_triggers[vc_id].discard(trigger.trigger_id)
                trigger_run_ids = trigger.generate_trigger_run_ids(execution_ctx)
                # Pick the first matched EventContext's event_id (if any) as the
                # parent event for the new invocation. For composite triggers
                # combining cron + event, this still preserves the event link.
                parent_event_id: str | None = None
                for vc in execution_ctx.valid_conditions.values():
                    inner = vc.context
                    if isinstance(inner, EventContext) and inner.event_id:
                        parent_event_id = inner.event_id
                        break
                for run_id in trigger_run_ids:
                    if not self.claim_trigger_run(run_id):
                        self.app.logger.debug(
                            join_tokens(
                                TriggerLogMsg.RUN_SKIPPED,
                                ref(
                                    "atomic-service-run",
                                    atomic_service_run.atomic_service_run_id,
                                ),
                                ref("trigger-run", run_id),
                                ref("trigger", trigger.trigger_id),
                                "reason:already-claimed",
                            )
                        )
                        continue
                    claimed_at = datetime.now(UTC)
                    self._log_run_claimed(
                        run_id, trigger, execution_ctx, atomic_service_run
                    )
                    args = trigger.get_arguments(execution_ctx)
                    invocation = self.execute_task(
                        trigger.task_id, args, parent_event_id=parent_event_id
                    )
                    executed_at = datetime.now(UTC)
                    self._record_trigger_run(
                        run_id=run_id,
                        trigger=trigger,
                        context=execution_ctx,
                        invocation=invocation,
                        claimed_at=claimed_at,
                        executed_at=executed_at,
                        atomic_service_run=atomic_service_run,
                    )
                    self._log_run_executed(
                        run_id, trigger, execution_ctx, invocation, atomic_service_run
                    )
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

    def _execution_contexts_for_trigger(
        self, trigger: TriggerDefinition, context: TriggerContext
    ) -> list[TriggerContext]:
        """Build the concrete contexts that should execute for ``trigger``.

        A trigger can receive several valid contexts for the same condition in
        one loop pass (for example four invocations of the same task reaching
        ``SUCCESS`` before the trigger loop wakes up). Single-condition triggers
        must execute once per valid context, not once for the whole batch.

        OR triggers also execute independently per satisfied condition so their
        argument provider and trigger-run record see the exact source context.
        Multi-condition AND triggers keep the combined context because they need
        one valid condition for each required condition.
        """
        if not trigger.should_trigger(context):
            return []
        relevant = [
            vc
            for vc in context.valid_conditions.values()
            if vc.condition.condition_id in trigger.condition_ids
        ]
        if trigger.logic == CompositeLogic.OR or len(trigger.condition_ids) == 1:
            return [
                TriggerContext(valid_conditions={vc.valid_condition_id: vc})
                for vc in relevant
            ]
        return [context]

    def execute_task(
        self,
        task_id: "TaskId",
        arguments: dict[str, Any] | None = None,
        *,
        parent_event_id: str | None = None,
    ) -> "BaseInvocation":
        """
        Execute a task with the given arguments.

        This method handles the actual execution of a task when its trigger conditions are met.

        :param task_id: ID of the task to execute
        :param arguments: Arguments to pass to the task, if any
        :param parent_event_id: Optional id of the event that fired this trigger.
            Set on the routed invocation so the family tree can show event-children.
        """
        # Get the task from the app
        task = self.app.get_task(task_id)
        if not task:
            raise ValueError(f"Task {task_id} not found")
        previous_event = ctx_mod.swap_trigger_event_context(
            self.app.app_id, parent_event_id
        )
        previous_inv = ctx_mod.swap_dist_invocation_context(self.app.app_id, None)
        try:
            invocation = task._call(Arguments(kwargs=arguments or {}))
        finally:
            ctx_mod.swap_dist_invocation_context(self.app.app_id, previous_inv)
            ctx_mod.swap_trigger_event_context(self.app.app_id, previous_event)
        self.app.logger.debug(f"Triggered task:{task_id} with arguments {arguments}")
        return invocation

    # ------------------------------------------------------------------ #
    # Trigger-run logging and monitoring records
    # ------------------------------------------------------------------ #
    # These helpers keep predefined logs and durable monitoring records aligned
    # so Pynmon can hydrate the same causal links from either source.

    def _log_run_claimed(
        self,
        run_id: str,
        trigger: TriggerDefinition,
        execution_ctx: TriggerContext,
        atomic_service_run: "AtomicServiceRun",
    ) -> None:
        """Emit the predefined ``trigger.run.claimed`` log.

        Lists the participating condition IDs so the Log Explorer can
        cross-link back to the matched conditions without an extra query.
        """
        condition_ids = [
            vc.condition.condition_id for vc in execution_ctx.valid_conditions.values()
        ]
        self.app.logger.info(
            join_tokens(
                TriggerLogMsg.RUN_CLAIMED,
                ref("atomic-service-run", atomic_service_run.atomic_service_run_id),
                ref("trigger-run", run_id),
                ref("trigger", trigger.trigger_id),
                f"task:{trigger.task_id}",
                f"logic:{trigger.logic.value}",
                ref_list("conditions", condition_ids),
            )
        )

    def _log_run_executed(
        self,
        run_id: str,
        trigger: TriggerDefinition,
        execution_ctx: TriggerContext,
        invocation: "BaseInvocation",
        atomic_service_run: "AtomicServiceRun",
    ) -> None:
        """Emit the predefined ``trigger.run.executed`` log.

        Includes the generated invocation plus any event/source-invocation
        participants so a single log line links every causal endpoint of the
        run for downstream Pynmon hydration.
        """
        event_ids: list[str] = []
        source_invocation_ids: list[str] = []
        for vc in execution_ctx.valid_conditions.values():
            inner = vc.context
            if isinstance(inner, EventContext) and inner.event_id:
                event_ids.append(inner.event_id)
            elif isinstance(inner, StatusContext):
                source_invocation_ids.append(str(inner.invocation_id))
        self.app.logger.info(
            join_tokens(
                TriggerLogMsg.RUN_EXECUTED,
                ref("atomic-service-run", atomic_service_run.atomic_service_run_id),
                ref("trigger-run", run_id),
                ref("trigger", trigger.trigger_id),
                f"task:{trigger.task_id}",
                f"logic:{trigger.logic.value}",
                ref("triggered-invocation", str(invocation.invocation_id)),
                ref_list("events", event_ids),
                ref_list("source-invocations", source_invocation_ids),
            )
        )

    def _record_trigger_run(
        self,
        *,
        run_id: str,
        trigger: TriggerDefinition,
        context: TriggerContext,
        invocation: "BaseInvocation",
        claimed_at: datetime,
        executed_at: datetime,
        atomic_service_run: "AtomicServiceRun",
    ) -> None:
        """Persist a :class:`TriggerRunRecord` and link affected events.

        Failures are logged and never propagated; monitoring must not break
        the trigger loop. The event/invocation relation is recorded via the
        backend's atomic ``link_trigger_run_to_events`` instead of a
        read-modify-write on the event JSON.
        """
        try:
            record = self._build_trigger_run_record(
                run_id,
                trigger,
                context,
                invocation,
                claimed_at,
                executed_at,
                atomic_service_run,
            )
            self.store_trigger_run(record)
            if record.event_ids:
                self.link_trigger_run_to_events(
                    record.event_ids,
                    str(invocation.invocation_id),
                    trigger_run_id=run_id,
                )
        except Exception:  # pragma: no cover - logged, never re-raised
            self.app.logger.exception(
                "trigger.store_trigger_run failed for run %s", run_id
            )

    def _build_trigger_run_record(
        self,
        run_id: str,
        trigger: TriggerDefinition,
        context: TriggerContext,
        invocation: "BaseInvocation",
        claimed_at: datetime,
        executed_at: datetime,
        atomic_service_run: "AtomicServiceRun",
    ) -> "TriggerRunRecord":
        """Assemble a :class:`TriggerRunRecord` from a satisfied trigger context."""
        from pynenc.trigger.monitoring import TriggerRunRecord

        atomic_service_run_id = atomic_service_run.atomic_service_run_id
        service_runner_id = atomic_service_run.runner_id

        monitoring_context = self._monitoring_context_for_trigger_run(trigger, context)
        participants = self._build_run_participants(monitoring_context)
        event_ids = [p.event_id for p in participants if p.event_id]
        source_invocation_ids = [
            p.source_invocation_id for p in participants if p.source_invocation_id
        ]
        return TriggerRunRecord(
            trigger_run_id=run_id,
            trigger_id=trigger.trigger_id,
            task_id_key=trigger.task_id.key,
            logic_value=trigger.logic.value,
            valid_condition_ids=list(monitoring_context.valid_conditions.keys()),
            condition_ids=[
                vc.condition.condition_id
                for vc in monitoring_context.valid_conditions.values()
            ],
            event_ids=event_ids,
            source_invocation_ids=source_invocation_ids,
            triggered_invocation_id=str(invocation.invocation_id),
            claimed_at=claimed_at or datetime.now(UTC),
            executed_at=executed_at or datetime.now(UTC),
            participants=participants,
            atomic_service_run_id=atomic_service_run_id,
            atomic_service_runner_id=service_runner_id,
        )

    def _monitoring_context_for_trigger_run(
        self,
        trigger: TriggerDefinition,
        context: TriggerContext,
    ) -> TriggerContext:
        """Keep only the condition instances that explain this trigger run."""
        relevant = [
            vc
            for vc in context.valid_conditions.values()
            if vc.condition.condition_id in trigger.condition_ids
        ]
        if trigger.logic != CompositeLogic.AND or len(trigger.condition_ids) < 2:
            return TriggerContext(
                valid_conditions={vc.valid_condition_id: vc for vc in relevant}
            )

        by_source: dict[str, list[ValidCondition]] = defaultdict(list)
        for vc in relevant:
            invocation_id = getattr(vc.context, "invocation_id", None)
            if invocation_id is not None:
                by_source[str(invocation_id)].append(vc)

        coherent_sources = {
            source_id: conditions
            for source_id, conditions in by_source.items()
            if len({vc.condition.condition_id for vc in conditions}) > 1
        }
        if not coherent_sources:
            return TriggerContext(
                valid_conditions={vc.valid_condition_id: vc for vc in relevant}
            )

        source_id, source_conditions = max(
            coherent_sources.items(),
            key=lambda item: (
                len({vc.condition.condition_id for vc in item[1]}),
                max(vc.context.timestamp for vc in item[1]),
                item[0],
            ),
        )
        correlated_condition_ids = {
            vc.condition.condition_id for vc in source_conditions
        }
        selected = [
            vc
            for vc in relevant
            if vc.condition.condition_id not in correlated_condition_ids
            or str(getattr(vc.context, "invocation_id", "")) == source_id
        ]
        return TriggerContext(
            valid_conditions={vc.valid_condition_id: vc for vc in selected}
        )

    def _build_run_participants(
        self, context: TriggerContext
    ) -> list["TriggerRunParticipant"]:
        """Build a per-condition participant list from ``context``.

        Stores only reference ids plus a lightweight timestamp/summary so
        pynmon timeline overlays and tooltips can anchor and label the
        participant. Detail panels re-fetch the live condition / event /
        invocation when they need the full payload.
        """
        from pynenc.trigger.monitoring import TriggerRunParticipant

        participants: list[TriggerRunParticipant] = []
        for vc in context.valid_conditions.values():
            ctx = vc.context
            event_id = ctx.event_id if isinstance(ctx, EventContext) else None
            source_inv = (
                str(ctx.invocation_id)
                if isinstance(ctx, (StatusContext, ResultContext, ExceptionContext))
                else None
            )
            participants.append(
                TriggerRunParticipant(
                    context_type=type(ctx).__name__,
                    condition_id=vc.condition.condition_id,
                    valid_condition_id=vc.valid_condition_id,
                    event_id=event_id,
                    source_invocation_id=source_inv,
                    context_timestamp=getattr(ctx, "timestamp", None),
                    context_summary=join_tokens(*context_extra_tokens(ctx)),
                )
            )
        return participants

    # ------------------------------------------------------------------ #
    # Full purge lifecycle
    # ------------------------------------------------------------------ #
    # A full purge removes backend state, then reloads task-declared trigger
    # definitions so the app can continue from a clean storage snapshot.

    def purge(self) -> None:
        """Purges all the trigger data for the current self.app.app_id."""
        self._purge()
        self.reload_task_conditions()

    @abstractmethod
    def _purge(self) -> None:
        """Purges all the trigger data for the current self.app.app_id."""

    # ------------------------------------------------------------------ #
    # Monitoring API
    # ------------------------------------------------------------------ #

    @abstractmethod
    def store_event(self, event: "EventRecord") -> None:
        """Store or replace an ``EventRecord``."""

    @abstractmethod
    def get_event(self, event_id: str) -> "EventRecord | None":
        """Return the ``EventRecord`` for ``event_id``, or ``None``."""

    @abstractmethod
    def get_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
        limit: int = 100,
        offset: int = 0,
    ) -> list["EventRecord"]:
        """Return events ordered by ``timestamp`` descending."""

    @abstractmethod
    def count_events(
        self,
        *,
        event_code: str | None = None,
        start_time: datetime | None = None,
        end_time: datetime | None = None,
        matched: bool | None = None,
        triggered: bool | None = None,
        emitted_by_invocation_id: str | None = None,
        emitted_by_task_id: str | None = None,
    ) -> int:
        """Count events with the same filters as ``get_events``."""

    @abstractmethod
    def get_event_markers_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        state: str = "all",
        limit: int = 1000,
        offset: int = 0,
    ) -> "EventMarkerPage":
        """Return a paginated :class:`EventMarkerPage` for the time window.

        Use a narrow projection: id, code, timestamp, matched, triggered.

        :param state: ``"all"``, ``"matched"``, ``"unmatched"``,
            ``"triggered"``, or ``"untriggered"``.
        """

    @abstractmethod
    def link_trigger_run_to_events(
        self,
        event_ids: list[str],
        invocation_id: str,
        *,
        trigger_run_id: str,
    ) -> None:
        """Link ``invocation_id`` to ``event_ids``.

        Backends should update event links atomically.
        """

    @abstractmethod
    def get_invocations_triggered_by_event(self, event_id: str) -> list[str]:
        """Return invocation IDs triggered by ``event_id``."""

    @abstractmethod
    def list_event_codes(self) -> list[str]:
        """Return the distinct event codes."""

    @abstractmethod
    def store_trigger_run(self, run: "TriggerRunRecord") -> None:
        """Store or replace a ``TriggerRunRecord``."""

    @abstractmethod
    def get_trigger_run(self, trigger_run_id: str) -> "TriggerRunRecord | None":
        """Return the ``TriggerRunRecord`` for ``trigger_run_id``, or ``None``."""

    @abstractmethod
    def get_trigger_runs_for_event(self, event_id: str) -> list["TriggerRunRecord"]:
        """Return trigger runs that reference ``event_id``."""

    @abstractmethod
    def get_trigger_runs_for_invocation(
        self, invocation_id: str
    ) -> list["TriggerRunRecord"]:
        """Return trigger runs that produced ``invocation_id``."""

    def get_trigger_runs_for_valid_condition(
        self, valid_condition_id: str
    ) -> list["TriggerRunRecord"]:
        """Return trigger runs that include ``valid_condition_id``.

        Historical participant snapshots keep the match visible.
        """
        runs = self.get_trigger_runs_in_timerange(
            start_time=datetime.min.replace(tzinfo=UTC),
            end_time=datetime.max.replace(tzinfo=UTC),
        )
        return [
            run
            for run in runs
            if valid_condition_id in run.valid_condition_ids
            or any(
                participant.valid_condition_id == valid_condition_id
                for participant in run.participants or []
            )
        ]

    @abstractmethod
    def get_trigger_runs_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        task_id_key: str | None = None,
        limit: int | None = None,
    ) -> list["TriggerRunRecord"]:
        """Return trigger runs between ``start_time`` and ``end_time``."""

    def get_trigger_runs_sourced_by_invocation(
        self, invocation_id: str
    ) -> list["TriggerRunRecord"]:
        """Return runs where ``invocation_id`` was a source participant.

        Backends with a source-invocation index should override this scan.
        """
        matches: list[TriggerRunRecord] = []
        seen: set[str] = set()
        for run in self.get_trigger_runs_in_timerange(
            start_time=datetime.min.replace(tzinfo=UTC),
            end_time=datetime.max.replace(tzinfo=UTC),
        ):
            if invocation_id not in run.source_invocation_ids:
                continue
            if run.trigger_run_id in seen:
                continue
            seen.add(run.trigger_run_id)
            matches.append(run)
        return matches

    def get_events_batch(
        self, event_ids: Sequence[str]
    ) -> dict[str, "EventRecord | None"]:
        """Return ``EventRecord`` values for each id, or ``None``."""
        return {eid: self.get_event(eid) for eid in event_ids}

    def get_trigger_runs_batch(
        self, run_ids: Sequence[str]
    ) -> dict[str, "TriggerRunRecord | None"]:
        """Return ``TriggerRunRecord`` values for each id, or ``None``."""
        return {rid: self.get_trigger_run(rid) for rid in run_ids}

    def get_referenced_atomic_service_run_ids(self) -> set[str]:
        """Return ``atomic_service_run_id`` values referenced by trigger runs.

        Used to avoid purging linked execution records. Override with a
        dedicated index when available.
        """
        referenced: set[str] = set()
        for run in self.get_trigger_runs_in_timerange(
            start_time=datetime.min.replace(tzinfo=UTC),
            end_time=datetime.max.replace(tzinfo=UTC),
        ):
            if run.atomic_service_run_id:
                referenced.add(run.atomic_service_run_id)
        return referenced

    def get_conditions_batch(
        self, condition_ids: Sequence[str]
    ) -> dict[str, "TriggerCondition | None"]:
        """Return ``TriggerCondition`` values for each id, or ``None``."""
        return {cid: self.get_condition(cid) for cid in condition_ids}

    def get_triggers_batch(
        self, trigger_ids: Sequence[str]
    ) -> dict[str, "TriggerDefinition | None"]:
        """Return ``TriggerDefinition`` values for each id, or ``None``."""
        return {tid: self.get_trigger(tid) for tid in trigger_ids}

    def get_events_emitted_by_invocation(
        self,
        invocation_id: str,
        *,
        limit: int = 200,
        offset: int = 0,
    ) -> list["EventRecord"]:
        """Return events emitted by ``invocation_id``."""
        return self.get_events(
            emitted_by_invocation_id=invocation_id,
            limit=limit,
            offset=offset,
        )

    def auto_purge_events(self) -> int:
        """
        Purge expired event and trigger-run records.

        Returns ``0`` when auto purge is disabled.
        """
        if not self.conf.event_auto_purge_enabled:
            return 0
        return self._auto_purge_events()

    def _auto_purge_events(self) -> int:
        """
        Apply age and capacity purge to events and trigger runs.
        """
        now = datetime.now(UTC)
        event_threshold = now - timedelta(days=self.conf.event_retention_days)
        run_threshold = event_threshold

        removed_event_ids = self._age_purge_events(event_threshold)
        deleted = len(removed_event_ids)
        deleted += self._age_purge_trigger_runs(run_threshold)
        extra_event_ids = self._cap_purge_events()
        removed_event_ids.extend(extra_event_ids)
        deleted += len(extra_event_ids)
        if removed_event_ids:
            deleted += self._cascade_delete_runs_for_events(removed_event_ids)
        deleted += self._cap_purge_trigger_runs()
        return deleted

    @abstractmethod
    def _age_purge_events(self, threshold: datetime) -> list[str]:
        """Delete events older than ``threshold`` and return their ids."""

    @abstractmethod
    def _age_purge_trigger_runs(self, threshold: datetime) -> int:
        """Delete trigger runs older than ``threshold`` and return the count."""

    @abstractmethod
    def _cap_purge_events(self) -> list[str]:
        """Delete excess events and return their ids."""

    @abstractmethod
    def _cascade_delete_runs_for_events(self, event_ids: list[str]) -> int:
        """Delete trigger runs that reference any of ``event_ids``."""

    @abstractmethod
    def _cap_purge_trigger_runs(self) -> int:
        """Delete excess trigger runs and return the count."""

    # ------------------------------------------------------------------ #
    # Task reload helpers
    # ------------------------------------------------------------------ #
    # Reloading is intentionally local to the trigger component: tasks keep
    # their trigger builders, while this class rebuilds runtime indexes.

    def reload_task_conditions(self) -> None:
        """
        Reload all task conditions from the current app's registered tasks.

        This method clears the local condition and source-task mappings,
        then iterates over all tasks in the app and re-registers their triggers and conditions.

        :return: None
        """
        # Clear local mappings
        self._last_cron_execution_cache.clear()
        self._registered_conditions = {}
        self._source_task_conditions = defaultdict(set)

        # Re-register triggers and conditions for each task
        for task in self.app.tasks.values():
            # This will re-register all triggers and their conditions for the task
            self.register_task_triggers(task, getattr(task, "triggers", None))
