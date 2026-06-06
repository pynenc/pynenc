"""
In-memory implementation of the Pynenc trigger subsystem.

This module provides an implementation of the trigger system that stores
all its state in memory. It's suitable for development and testing purposes.
"""

import threading
from collections import defaultdict
from collections.abc import Callable, Iterable
from datetime import UTC, datetime, timedelta
from typing import TYPE_CHECKING, TypeVar

from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import ConditionContext, TriggerCondition, ValidCondition
from pynenc.trigger.monitoring import (
    EventMarker,
    EventMarkerPage,
    EventRecord,
    TriggerRunRecord,
)

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.identifiers.task_id import TaskId
    from pynenc.models.trigger_definition_dto import TriggerDefinitionDTO
    from pynenc.trigger.types import ConditionId


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
        self._triggers: dict[str, TriggerDefinitionDTO] = {}
        # Map of condition_id to list of trigger_ids that use this condition
        self._condition_triggers: dict[str, list[str]] = defaultdict(list)
        # Map of condition_id to valid condition
        self._valid_conditions: dict[str, ValidCondition] = {}
        # Map of task_id to set of condition_ids that are sourced from this task
        self._source_task_conditions: dict[TaskId, set[str]] = defaultdict(set)
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
        # Monitoring stores: events and trigger runs
        self._events: dict[str, EventRecord] = {}
        self._trigger_runs: dict[str, TriggerRunRecord] = {}
        # Indexes maintained alongside the monitoring stores. They support
        # the dashboard query API without scanning the full dictionaries.
        self._event_ids_by_code: dict[str, set[str]] = defaultdict(set)
        self._event_ids_by_emitter: dict[str, set[str]] = defaultdict(set)
        self._run_ids_by_event_id: dict[str, set[str]] = defaultdict(set)
        self._run_ids_by_invocation_id: dict[str, set[str]] = defaultdict(set)
        self._run_ids_by_source_invocation_id: dict[str, set[str]] = defaultdict(set)
        self._run_ids_by_valid_condition_id: dict[str, set[str]] = defaultdict(set)
        # Source-of-truth for the event -> triggered-invocation relation.
        # ``EventRecord.triggered_invocation_ids`` is rehydrated from here on
        # read instead of being mutated in place on the JSON payload.
        self._invocations_by_event_id: dict[str, list[str]] = defaultdict(list)
        self._monitoring_lock = threading.RLock()

    # ------------------------------------------------------------------ #
    # Registration storage
    # ------------------------------------------------------------------ #
    # These methods back BaseTrigger registration with in-memory dictionaries
    # and reverse indexes used by status/result/exception reports.

    def _register_condition(self, condition: TriggerCondition) -> None:
        """
        Register a condition in the in-memory system.

        :param condition: The condition to register
        """
        condition_id = condition.condition_id
        self._conditions[condition_id] = condition

    def get_condition(self, condition_id: str) -> TriggerCondition | None:
        """
        Get a condition by its ID from the in-memory store.

        :param condition_id: ID of the condition to retrieve
        :return: The condition if found, None otherwise
        """
        return self._conditions.get(condition_id)

    def register_trigger(self, trigger: "TriggerDefinitionDTO") -> None:
        """
        Register a trigger definition in the in-memory system.

        :param trigger: The trigger definition to register
        """
        self._triggers[trigger.trigger_id] = trigger

        # Map each condition to this trigger
        for condition_id in trigger.condition_ids:
            self._condition_triggers[condition_id].append(trigger.trigger_id)

    def _get_trigger(self, trigger_id: str) -> "TriggerDefinitionDTO | None":
        """
        Get a trigger definition by ID from the in-memory store.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        return self._triggers.get(trigger_id)

    def get_triggers_for_condition(
        self, condition_id: str
    ) -> list["TriggerDefinitionDTO"]:
        """
        Get all triggers that depend on a specific condition from the in-memory store.

        :param condition_id: ID of the condition
        :return: List of trigger definitions using this condition
        """
        trigger_ids = self._condition_triggers.get(condition_id, [])
        return [self._triggers[tid] for tid in trigger_ids if tid in self._triggers]

    # ------------------------------------------------------------------ #
    # Valid-condition storage
    # ------------------------------------------------------------------ #
    # Valid conditions are transient loop inputs. The memory backend keeps
    # them copyable so the loop can clear only contexts that were consumed.

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

    # ------------------------------------------------------------------ #
    # Cron scheduling state
    # ------------------------------------------------------------------ #
    # The cron lock protects the optimistic last-execution check used by the
    # base trigger to avoid duplicate scheduled executions in threaded tests.

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

    # ------------------------------------------------------------------ #
    # Source-task indexes and trigger-run claims
    # ------------------------------------------------------------------ #
    # Source-task indexes answer "which conditions care about this task?";
    # claim dictionaries prevent two loop iterations from executing the same
    # logical trigger run while a previous claim is still valid.

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
        now = datetime.now(UTC)
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
        now = datetime.now(UTC)
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

    # ------------------------------------------------------------------ #
    # Task trigger cleanup and full purge
    # ------------------------------------------------------------------ #
    # Task cleanup removes definitions while preserving unrelated monitoring
    # records; full purge clears every runtime store for this backend.

    def clean_task_trigger_definitions(self, task_id: "TaskId") -> None:
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

    def _purge(self) -> None:
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
        with self._monitoring_lock:
            self._events.clear()
            self._trigger_runs.clear()
            self._event_ids_by_code.clear()
            self._event_ids_by_emitter.clear()
            self._run_ids_by_event_id.clear()
            self._run_ids_by_invocation_id.clear()
            self._run_ids_by_source_invocation_id.clear()
            self._run_ids_by_valid_condition_id.clear()
            self._invocations_by_event_id.clear()

    # ------------------------------------------------------------------ #
    # Monitoring API
    # ------------------------------------------------------------------ #

    # Event records and event indexes. Records are stored once and hydrated
    # with relation data from dedicated indexes on read.

    def store_event(self, event: EventRecord) -> None:
        with self._monitoring_lock:
            self._events[event.event_id] = event
            self._event_ids_by_code[event.event_code].add(event.event_id)
            if event.emitted_by_invocation_id:
                self._event_ids_by_emitter[event.emitted_by_invocation_id].add(
                    event.event_id
                )
            # Backward-compat: seed the invocation index from the record
            # field when callers populate it directly (legacy code or tests).
            if event.triggered_invocation_ids:
                bucket = self._invocations_by_event_id.setdefault(event.event_id, [])
                for inv in event.triggered_invocation_ids:
                    if inv not in bucket:
                        bucket.append(inv)

    def get_event(self, event_id: str) -> EventRecord | None:
        with self._monitoring_lock:
            record = self._events.get(event_id)
            if record is None:
                return None
            record.triggered_invocation_ids = list(
                self._invocations_by_event_id.get(event_id, ())
            )
            return record

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
    ) -> list[EventRecord]:
        records = self._select_event_records(
            event_code=event_code,
            emitted_by_invocation_id=emitted_by_invocation_id,
        )
        filtered = [
            r
            for r in records
            if _event_matches_filters(
                r,
                event_code,
                start_time,
                end_time,
                matched,
                triggered,
                emitted_by_invocation_id,
                emitted_by_task_id,
                triggered_lookup=self._invocations_by_event_id,
            )
        ]
        filtered.sort(key=lambda r: r.timestamp, reverse=True)
        page = filtered[offset : offset + limit]
        for record in page:
            record.triggered_invocation_ids = list(
                self._invocations_by_event_id.get(record.event_id, ())
            )
        return page

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
        records = self._select_event_records(
            event_code=event_code,
            emitted_by_invocation_id=emitted_by_invocation_id,
        )
        return sum(
            1
            for r in records
            if _event_matches_filters(
                r,
                event_code,
                start_time,
                end_time,
                matched,
                triggered,
                emitted_by_invocation_id,
                emitted_by_task_id,
                triggered_lookup=self._invocations_by_event_id,
            )
        )

    def _select_event_records(
        self,
        *,
        event_code: str | None,
        emitted_by_invocation_id: str | None,
    ) -> list[EventRecord]:
        """Return candidate event records using the narrowest available index."""
        with self._monitoring_lock:
            if emitted_by_invocation_id is not None:
                ids = self._event_ids_by_emitter.get(emitted_by_invocation_id, set())
                return [self._events[i] for i in ids if i in self._events]
            if event_code is not None:
                ids = self._event_ids_by_code.get(event_code, set())
                return [self._events[i] for i in ids if i in self._events]
            return list(self._events.values())

    def list_event_codes(self) -> list[str]:
        with self._monitoring_lock:
            return sorted(self._event_ids_by_code.keys())

    def get_event_markers_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        state: str = "all",
        limit: int = 1000,
        offset: int = 0,
    ) -> EventMarkerPage:
        with self._monitoring_lock:
            records = list(self._events.values())
            triggered_lookup = self._invocations_by_event_id
        filtered = [
            r
            for r in records
            if _marker_matches(
                r, start_time, end_time, event_code, state, triggered_lookup
            )
        ]
        filtered.sort(key=lambda r: r.timestamp)
        total = len(filtered)
        window = filtered[offset : offset + limit]
        markers = [
            EventMarker(
                event_id=r.event_id,
                event_code=r.event_code,
                timestamp=r.timestamp,
                matched=r.matched,
                triggered=bool(triggered_lookup.get(r.event_id)),
                emitted_by_invocation_id=r.emitted_by_invocation_id,
            )
            for r in window
        ]
        return EventMarkerPage(
            markers=markers,
            total=total,
            truncated=offset + len(markers) < total,
        )

    def link_trigger_run_to_events(
        self,
        event_ids: list[str],
        invocation_id: str,
        *,
        trigger_run_id: str,
    ) -> None:
        with self._monitoring_lock:
            for event_id in event_ids:
                bucket = self._invocations_by_event_id.setdefault(event_id, [])
                if invocation_id not in bucket:
                    bucket.append(invocation_id)

    def get_invocations_triggered_by_event(self, event_id: str) -> list[str]:
        with self._monitoring_lock:
            return list(self._invocations_by_event_id.get(event_id, ()))

    # Trigger-run records and indexes. The indexes mirror the common Pynmon
    # questions: produced invocation, source invocation, event, and condition.

    def store_trigger_run(self, run: TriggerRunRecord) -> None:
        with self._monitoring_lock:
            self._trigger_runs[run.trigger_run_id] = run
            for event_id in run.event_ids:
                self._run_ids_by_event_id[event_id].add(run.trigger_run_id)
            if run.triggered_invocation_id:
                self._run_ids_by_invocation_id[run.triggered_invocation_id].add(
                    run.trigger_run_id
                )
            for src in run.source_invocation_ids:
                self._run_ids_by_source_invocation_id[src].add(run.trigger_run_id)
            for valid_condition_id in run.valid_condition_ids:
                self._run_ids_by_valid_condition_id[valid_condition_id].add(
                    run.trigger_run_id
                )
            for participant in run.participants or []:
                if participant.valid_condition_id:
                    self._run_ids_by_valid_condition_id[
                        participant.valid_condition_id
                    ].add(run.trigger_run_id)

    def get_trigger_run(self, trigger_run_id: str) -> TriggerRunRecord | None:
        with self._monitoring_lock:
            return self._trigger_runs.get(trigger_run_id)

    def get_trigger_runs_for_event(self, event_id: str) -> list[TriggerRunRecord]:
        with self._monitoring_lock:
            ids = self._run_ids_by_event_id.get(event_id, set())
            runs = [self._trigger_runs[i] for i in ids if i in self._trigger_runs]
        runs.sort(key=lambda r: r.executed_at or r.claimed_at or datetime.min)
        return runs

    def get_trigger_runs_for_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        with self._monitoring_lock:
            ids = self._run_ids_by_invocation_id.get(invocation_id, set())
            return [self._trigger_runs[i] for i in ids if i in self._trigger_runs]

    def get_trigger_runs_sourced_by_invocation(
        self, invocation_id: str
    ) -> list[TriggerRunRecord]:
        with self._monitoring_lock:
            ids = self._run_ids_by_source_invocation_id.get(invocation_id, set())
            runs = [self._trigger_runs[i] for i in ids if i in self._trigger_runs]
        runs.sort(key=lambda r: r.executed_at or r.claimed_at or datetime.min)
        return runs

    def get_trigger_runs_for_valid_condition(
        self, valid_condition_id: str
    ) -> list[TriggerRunRecord]:
        with self._monitoring_lock:
            ids = self._run_ids_by_valid_condition_id.get(valid_condition_id, set())
            runs = [self._trigger_runs[i] for i in ids if i in self._trigger_runs]
        runs.sort(key=lambda r: r.executed_at or r.claimed_at or datetime.min)
        return runs

    def get_trigger_runs_in_timerange(
        self,
        start_time: datetime,
        end_time: datetime,
        *,
        event_code: str | None = None,
        task_id_key: str | None = None,
        limit: int | None = None,
    ) -> list[TriggerRunRecord]:
        with self._monitoring_lock:
            runs = list(self._trigger_runs.values())
        out: list[TriggerRunRecord] = []
        for run in runs:
            ts = run.executed_at or run.claimed_at
            if ts is None or ts < start_time or ts > end_time:
                continue
            if task_id_key is not None and run.task_id_key != task_id_key:
                continue
            if event_code is not None and not self._run_touches_event_code(
                run, event_code
            ):
                continue
            out.append(run)
        out.sort(key=lambda r: r.executed_at or r.claimed_at or datetime.min)
        if limit is not None:
            out = out[:limit]
        return out

    def _run_touches_event_code(self, run: TriggerRunRecord, event_code: str) -> bool:
        """True when any event referenced by ``run`` has the given code."""
        with self._monitoring_lock:
            return any(
                self._events[eid].event_code == event_code
                for eid in run.event_ids
                if eid in self._events
            )

    # ------------------------------------------------------------------ #
    # Monitoring retention
    # ------------------------------------------------------------------ #
    # Retention deletes old/capacity-excess events first, cascades dependent
    # runs, then rebuilds indexes so all read paths keep the same view.
    # The driving algorithm lives in BaseTrigger._auto_purge_events; this
    # class supplies the in-memory primitives.

    def _age_purge_events(self, threshold: datetime) -> list[str]:
        with self._monitoring_lock:
            removed = _purge_dict_keys_by_predicate(
                self._events, lambda r: r.timestamp < threshold
            )
            self._drop_event_indexes(removed)
        return removed

    def _age_purge_trigger_runs(self, threshold: datetime) -> int:
        now = datetime.now(UTC)
        with self._monitoring_lock:
            return _purge_dict_by_predicate(
                self._trigger_runs,
                lambda r: (r.executed_at or r.claimed_at or now) < threshold,
            )

    def _cap_purge_events(self) -> list[str]:
        with self._monitoring_lock:
            removed = _enforce_capacity(
                self._events,
                self.conf.event_max_records,
                key=lambda r: r.timestamp,
            )
            self._drop_event_indexes(removed)
        return removed

    def _cap_purge_trigger_runs(self) -> int:
        with self._monitoring_lock:
            removed = _enforce_capacity(
                self._trigger_runs,
                self.conf.trigger_run_max_records,
                key=lambda r: r.executed_at or r.claimed_at or datetime.min,
            )
            self._rebuild_run_indexes()
        return len(removed)

    def _drop_event_indexes(self, event_ids: list[str]) -> None:
        """Remove every cached lookup entry for ``event_ids``."""
        for event_id in event_ids:
            self._invocations_by_event_id.pop(event_id, None)
            self._run_ids_by_event_id.pop(event_id, None)
        for code, ids in list(self._event_ids_by_code.items()):
            ids.difference_update(event_ids)
            if not ids:
                del self._event_ids_by_code[code]
        for emitter, ids in list(self._event_ids_by_emitter.items()):
            ids.difference_update(event_ids)
            if not ids:
                del self._event_ids_by_emitter[emitter]

    def _rebuild_run_indexes(self) -> None:
        """Recompute trigger-run indexes after a purge sweep."""
        self._run_ids_by_event_id.clear()
        self._run_ids_by_invocation_id.clear()
        self._run_ids_by_source_invocation_id.clear()
        self._run_ids_by_valid_condition_id.clear()
        for run in self._trigger_runs.values():
            for event_id in run.event_ids:
                self._run_ids_by_event_id[event_id].add(run.trigger_run_id)
            if run.triggered_invocation_id:
                self._run_ids_by_invocation_id[run.triggered_invocation_id].add(
                    run.trigger_run_id
                )
            for src in run.source_invocation_ids:
                self._run_ids_by_source_invocation_id[src].add(run.trigger_run_id)
            for valid_condition_id in run.valid_condition_ids:
                self._run_ids_by_valid_condition_id[valid_condition_id].add(
                    run.trigger_run_id
                )
            for participant in run.participants or []:
                if participant.valid_condition_id:
                    self._run_ids_by_valid_condition_id[
                        participant.valid_condition_id
                    ].add(run.trigger_run_id)

    def _cascade_delete_runs_for_events(self, event_ids: list[str]) -> int:
        """Delete trigger-run records that reference any of ``event_ids``."""
        targets = set(event_ids)
        to_drop = [
            rid
            for rid, run in self._trigger_runs.items()
            if any(eid in targets for eid in run.event_ids)
        ]
        for rid in to_drop:
            del self._trigger_runs[rid]
        return len(to_drop)


# ---------------------------------------------------------------------- #
# Pure in-memory filter and retention helpers
# ---------------------------------------------------------------------- #
# Kept outside the class because they do not need backend state beyond the
# dictionaries passed to them.


_T = TypeVar("_T")


def _event_matches_filters(
    record: EventRecord,
    event_code: str | None,
    start_time: datetime | None,
    end_time: datetime | None,
    matched: bool | None,
    triggered: bool | None,
    emitted_by_invocation_id: str | None = None,
    emitted_by_task_id: str | None = None,
    *,
    triggered_lookup: dict[str, list[str]] | None = None,
) -> bool:
    """Return True when ``record`` satisfies all provided filters."""
    if event_code is not None and record.event_code != event_code:
        return False
    if start_time is not None and record.timestamp < start_time:
        return False
    if end_time is not None and record.timestamp > end_time:
        return False
    if matched is not None and record.matched != matched:
        return False
    if emitted_by_invocation_id is not None and (
        record.emitted_by_invocation_id != emitted_by_invocation_id
    ):
        return False
    if emitted_by_task_id is not None and (
        record.emitted_by_task_id != emitted_by_task_id
    ):
        return False
    if triggered is not None:
        is_triggered = bool(
            triggered_lookup.get(record.event_id)
            if triggered_lookup is not None
            else record.triggered_invocation_ids
        )
        if is_triggered != triggered:
            return False
    return True


def _marker_matches(
    record: EventRecord,
    start_time: datetime,
    end_time: datetime,
    event_code: str | None,
    state: str,
    triggered_lookup: dict[str, list[str]],
) -> bool:
    """True when the marker projection of ``record`` matches the window/state."""
    if record.timestamp < start_time or record.timestamp > end_time:
        return False
    if event_code is not None and record.event_code != event_code:
        return False
    triggered = bool(triggered_lookup.get(record.event_id))
    if state == "matched" and not record.matched:
        return False
    if state == "unmatched" and record.matched:
        return False
    if state == "triggered" and not triggered:
        return False
    if state == "untriggered" and triggered:
        return False
    return True


def _purge_dict_by_predicate(
    store: dict[str, _T], predicate: Callable[[_T], bool]
) -> int:
    """Drop entries matching ``predicate`` from ``store``; return delete count."""
    return len(_purge_dict_keys_by_predicate(store, predicate))


def _purge_dict_keys_by_predicate(
    store: dict[str, _T], predicate: Callable[[_T], bool]
) -> list[str]:
    """Drop entries matching ``predicate``; return the dropped keys."""
    to_drop = [key for key, value in store.items() if predicate(value)]
    for key in to_drop:
        del store[key]
    return to_drop


def _enforce_capacity(
    store: dict[str, _T], max_records: int, *, key: Callable[[_T], datetime]
) -> list[str]:
    """Keep only the ``max_records`` newest entries; return the dropped keys."""
    if max_records <= 0 or len(store) <= max_records:
        return []
    ordered = sorted(store.items(), key=lambda item: key(item[1]), reverse=True)
    to_keep = {item[0] for item in ordered[:max_records]}
    to_drop = [key_id for key_id in store if key_id not in to_keep]
    for key_id in to_drop:
        del store[key_id]
    return to_drop
