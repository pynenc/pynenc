"""
Redis-backed implementation of the trigger system.

This module provides a distributed trigger system implementation using Redis
for persistence and coordination across multiple application instances.
"""

from collections.abc import Iterable
from datetime import datetime, timezone
from functools import cached_property
from typing import TYPE_CHECKING, Optional

import redis

from pynenc.conf.config_trigger import ConfigTriggerRedis
from pynenc.trigger.base_trigger import BaseTrigger
from pynenc.trigger.conditions import TriggerCondition, ValidCondition
from pynenc.trigger.trigger_definitions import TriggerDefinition
from pynenc.trigger.types import ConditionId
from pynenc.util.redis_client import get_redis_client
from pynenc.util.redis_keys import Key

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.conditions import ConditionContext


class RedisTrigger(BaseTrigger):
    """
    Redis-backed implementation of the trigger system.

    This implementation uses Redis to store trigger conditions and definitions,
    making it suitable for distributed systems where multiple application instances
    need coordinated trigger behavior with persistence and reliability.
    """

    def __init__(self, app: "Pynenc") -> None:
        """
        Initialize the Redis-based trigger component.

        :param app: The Pynenc application instance
        """
        super().__init__(app)
        self._client: redis.Redis | None = None
        self.key = Key(app.app_id, "trigger")

    @cached_property
    def conf(self) -> ConfigTriggerRedis:
        """
        Get the Redis trigger configuration.

        :return: Configuration for Redis trigger
        """
        return ConfigTriggerRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def client(self) -> redis.Redis:
        """
        Lazy initialization of Redis client.

        :return: Redis client instance
        """
        if self._client is None:
            self.app.logger.debug("Lazy initializing Redis client for trigger system")
            self._client = get_redis_client(self.conf)
        return self._client

    def _register_condition(self, condition: TriggerCondition) -> None:
        """
        Register a condition in Redis.

        :param condition: The condition to register
        """
        condition_id = condition.condition_id
        self.client.set(
            self.key.condition(condition_id),
            condition.to_json(self.app),
        )

    def get_condition(self, condition_id: str) -> Optional[TriggerCondition]:
        """
        Get a condition by its ID from Redis.

        :param condition_id: ID of the condition to retrieve
        :return: The condition if found, None otherwise
        """
        condition_data = self.client.get(self.key.condition(condition_id))
        if condition_data:
            return TriggerCondition.from_json(condition_data.decode(), self.app)
        return None

    def register_trigger(self, trigger: TriggerDefinition) -> None:
        """
        Register a trigger definition in Redis.

        :param trigger: The trigger definition to register
        """
        self.client.set(
            self.key.trigger(trigger.trigger_id),
            trigger.to_json(self.app),
        )

        # Map each condition to this trigger
        for condition_id in trigger.condition_ids:
            self.client.sadd(
                self.key.condition_triggers(condition_id),
                trigger.trigger_id,
            )

        # Register with task for easy lookup
        self.client.sadd(
            self.key.task_triggers(trigger.task_id),
            trigger.trigger_id,
        )

    def get_trigger(self, trigger_id: str) -> Optional[TriggerDefinition]:
        """
        Get a trigger definition by ID from Redis.

        :param trigger_id: ID of the trigger to retrieve
        :return: The trigger definition if found, None otherwise
        """
        trigger_data = self.client.get(self.key.trigger(trigger_id))
        if trigger_data:
            return TriggerDefinition.from_json(trigger_data.decode(), self.app)
        return None

    def get_triggers_for_condition(self, condition_id: str) -> list[TriggerDefinition]:
        """
        Get all triggers that depend on a specific condition from Redis.

        :param condition_id: ID of the condition
        :return: List of trigger definitions using this condition
        """
        trigger_ids = self.client.smembers(self.key.condition_triggers(condition_id))
        triggers = []

        for trigger_id in trigger_ids:
            trigger = self.get_trigger(trigger_id.decode())
            if trigger:
                triggers.append(trigger)

        return triggers

    def get_triggers_for_task(self, task_id: str) -> list[TriggerDefinition]:
        """
        Get all triggers associated with a specific task from Redis.

        :param task_id: ID of the task to find triggers for
        :return: List of trigger definitions for this task
        """
        trigger_ids = self.client.smembers(self.key.task_triggers(task_id))
        triggers = []
        for trigger_id in trigger_ids:
            trigger = self.get_trigger(trigger_id.decode())
            if trigger:
                triggers.append(trigger)

        return triggers

    def record_valid_condition(self, valid_condition: ValidCondition) -> None:
        """
        Record that a condition has been satisfied with a specific context in Redis.

        :param valid_condition: The valid condition to record
        """
        self.client.set(
            self.key.valid_condition(valid_condition.valid_condition_id),
            valid_condition.to_json(self.app),
        )

    def record_valid_conditions(self, valid_conditions: list[ValidCondition]) -> None:
        """
        Record that multiple conditions have been satisfied with their respective contexts in Redis.

        :param valid_conditions: The list of valid conditions to record
        """
        if not valid_conditions:
            return

        pipeline = self.client.pipeline()
        for condition in valid_conditions:
            pipeline.set(
                self.key.valid_condition(condition.valid_condition_id),
                condition.to_json(self.app),
            )
        pipeline.execute()

    def get_valid_conditions(self) -> dict[str, ValidCondition]:
        """
        Get all currently valid conditions and their contexts from Redis.

        :return: Dictionary mapping condition IDs to their valid conditions
        """
        valid_conditions: dict[str, ValidCondition] = {}

        # Get all valid condition keys
        keys_pattern = self.key.valid_condition("*")
        all_keys = self.client.keys(keys_pattern)

        if not all_keys:
            return valid_conditions

        # Get all valid conditions in a single operation
        pipeline = self.client.pipeline()
        for key in all_keys:
            pipeline.get(key)
        for data in pipeline.execute():
            if data:
                vc = ValidCondition.from_json(data.decode(), self.app)
                valid_conditions[vc.valid_condition_id] = vc
        return valid_conditions

    def clear_valid_conditions(self, conditions: Iterable["ValidCondition"]) -> None:
        """
        Clear valid conditions after they have been processed from Redis.

        :param conditions: List of valid conditions to clear
        """
        if not conditions:
            return

        pipeline = self.client.pipeline()
        for condition in conditions:
            pipeline.delete(self.key.valid_condition(condition.valid_condition_id))
        pipeline.execute()

    def _get_all_conditions(self) -> list[TriggerCondition]:
        """
        Get all registered conditions from Redis.

        :return: List of all conditions
        """
        conditions: list[TriggerCondition] = []

        # Get all condition keys
        keys_pattern = self.key.condition("*")
        all_keys = self.client.keys(keys_pattern)

        if not all_keys:
            return conditions

        # Get all conditions in a single operation
        pipeline = self.client.pipeline()
        for key in all_keys:
            pipeline.get(key)
        results = pipeline.execute()

        # Process the results
        for data in results:
            if data:
                condition = TriggerCondition.from_json(data.decode(), self.app)
                conditions.append(condition)

        return conditions

    def purge(self) -> None:
        """
        Purge all trigger-related data from Redis.

        Removes all conditions, triggers, and valid conditions for this application.
        """
        self.key.purge(self.client)

    def get_last_cron_execution(self, condition_id: ConditionId) -> datetime | None:
        """
        Get the timestamp of the last execution of a cron condition from Redis.

        :param condition_id: ID of the cron condition
        :return: Timestamp of last execution, or None if never executed
        """
        timestamp_str = self.client.get(self.key.cron_last_execution(condition_id))
        if not timestamp_str:
            return None

        try:
            # Parse the ISO format datetime string
            return datetime.fromisoformat(timestamp_str.decode())
        except (ValueError, AttributeError) as e:
            self.app.logger.error(f"Failed to parse timestamp {timestamp_str}: {e}")
            return None

    def store_last_cron_execution(
        self,
        condition_id: str,
        execution_time: datetime,
        expected_last_execution: Optional[datetime] = None,
    ) -> bool:
        """
        Store the last execution time for a cron condition with optimistic locking.

        Uses Redis atomic operations to ensure thread safety:
        1. For new records (no expected_last_execution): Uses SETNX for atomic create-if-not-exists
        2. For updating existing records: Uses optimistic locking with WATCH/MULTI/EXEC pattern

        :param condition_id: ID of the cron condition
        :param execution_time: Time of execution to store
        :param expected_last_execution: Expected current value (for optimistic locking)
        :return: True if update successful, False if another process updated first
        """
        key = self.key.cron_last_execution(condition_id)
        new_value = execution_time.isoformat()
        expected_value = (
            expected_last_execution.isoformat() if expected_last_execution else None
        )

        if expected_last_execution is None:
            # Case 1: No expected value - use SETNX for atomic create-if-not-exists
            return bool(self.client.setnx(key, new_value))
        else:
            # Case 2: Expected value provided - use optimistic locking
            # Start a transaction with WATCH
            pipe = self.client.pipeline()
            pipe.watch(key)
            current_value: str | bytes | None = pipe.get(key)  # type: ignore
            if current_value and isinstance(current_value, bytes):
                current_value = current_value.decode("utf-8")
            if current_value != expected_value:
                pipe.unwatch()
                return False
            # Value matches expected, proceed with update
            pipe.multi()
            pipe.set(key, new_value)
            # Execute returns None if transaction failed due to key modification
            results = pipe.execute()
            return bool(results and results[0])

    def _register_source_task_condition(
        self, task_id: str, condition_id: "ConditionId"
    ) -> None:
        """
        Register the conditions that are sourced from a task in Redis.

        This method stores a mapping from source task IDs to the condition IDs
        that monitor them, enabling efficient lookup when task status changes.

        :param task_id: ID of the source task
        :param condition_id: ID of the condition sourced from the task
        """
        self.client.sadd(
            self.key.source_task_conditions(task_id),
            condition_id,
        )

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
        condition_ids = self.client.smembers(self.key.source_task_conditions(task_id))
        conditions = []

        for condition_id in condition_ids:
            condition = self.get_condition(condition_id.decode())
            if condition:
                if context_type is None or condition.context_type == context_type:
                    conditions.append(condition)
        return conditions

    def claim_trigger_execution(
        self, trigger_id: str, valid_condition_id: str, expiration_seconds: int = 60
    ) -> bool:
        """
        Atomically claim the right to execute a trigger for a specific valid condition.

        Uses Redis's SETNX (SET if Not eXists) for atomic claim operations across multiple workers.
        The claim automatically expires after the specified seconds to prevent stale locks.

        :param trigger_id: ID of the trigger being executed
        :param valid_condition_id: ID of the valid condition being processed
        :param expiration_seconds: Number of seconds after which the claim expires
        :return: True if the claim was successful, False if another worker has claimed it
        """
        claim_key = self.key.trigger_execution_claim(trigger_id, valid_condition_id)

        # Try to set the key only if it doesn't exist (SETNX) with an expiration
        # Returns 1 if the key was set (claim successful), 0 otherwise
        result = self.client.set(
            claim_key,
            datetime.now(timezone.utc).isoformat(),
            nx=True,  # Only set if key doesn't exist (SETNX)
            ex=expiration_seconds,  # Set expiration time
        )

        return bool(result)

    def claim_trigger_run(
        self, trigger_run_id: str, expiration_seconds: int = 60
    ) -> bool:
        """
        Atomically claim the right to execute a trigger run.

        Uses Redis's SETNX (SET if Not eXists) for atomic claim operations across multiple workers.
        The claim automatically expires after the specified seconds to prevent stale locks.

        :param trigger_run_id: Unique ID for this trigger run
        :param expiration_seconds: Number of seconds after which the claim expires
        :return: True if the claim was successful, False if another worker has claimed it
        """
        claim_key = self.key.trigger_run_claim(trigger_run_id)

        # Try to set the key only if it doesn't exist (SETNX) with an expiration
        # Returns True if the key was set (claim successful), False otherwise
        result = self.client.set(
            claim_key,
            datetime.now(timezone.utc).isoformat(),
            nx=True,  # Only set if key doesn't exist (SETNX)
            ex=expiration_seconds,  # Set expiration time
        )

        return bool(result)

    def clean_task_trigger_definitions(self, task_id: str) -> None:
        """
        Remove all trigger definitions for a specific task from Redis.

        This method removes all trigger definitions associated with the given task
        and their references in the index keys. It's safe to use in a distributed
        environment as it uses Redis atomic operations.

        :param task_id: ID of the task to clean triggers for
        """
        # Get all trigger IDs for this task
        task_trigger_key = self.key.task_triggers(task_id)
        trigger_ids = self.client.smembers(task_trigger_key)

        if not trigger_ids:
            return

        pipeline = self.client.pipeline()

        for trigger_id in trigger_ids:
            trigger_data = self.client.get(self.key.trigger(trigger_id.decode()))
            if trigger_data:
                trigger = TriggerDefinition.from_json(trigger_data.decode(), self.app)
                for condition_id in trigger.condition_ids:
                    pipeline.srem(
                        self.key.condition_triggers(condition_id), trigger_id.decode()
                    )
                pipeline.delete(self.key.trigger(trigger_id.decode()))
        pipeline.delete(task_trigger_key)
        pipeline.execute()
