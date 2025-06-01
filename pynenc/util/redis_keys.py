from typing import TYPE_CHECKING

import redis

if TYPE_CHECKING:
    from pynenc.invocation.status import InvocationStatus

PYNENC_KEY_PREFIX = "__pynenc__"


def sanitize_for_redis(s: str) -> str:
    """
    Sanitizes a string for use as a Redis key.

    :param str s: The string to sanitize.
    :return: The sanitized string.
    """
    if s is None:
        return ""
    replacements = {
        "[": "__OPEN_BRACKET__",
        "]": "__CLOSE_BRACKET__",
        "*": "__ASTERISK__",
    }
    for k, v in replacements.items():
        s = s.replace(k, v)
    return s


class Key:
    """
    Helper class to manage Redis key formats for various components.

    :param str app_id: The application ID.
    :param str prefix: The prefix for the keys.
    """

    def __init__(self, app_id: str, prefix: str) -> None:
        prefix = sanitize_for_redis(prefix)
        if not prefix:
            raise ValueError("Prefix cannot be an empty string or None")
        app_id = sanitize_for_redis(app_id)
        if not app_id:
            raise ValueError("App ID cannot be an empty string or None")
        if ":" in app_id:
            raise ValueError("App ID cannot contain ':'")
        if prefix and not prefix.endswith(":"):
            prefix += ":"
        self.class_prefix = prefix
        self.prefix = f"{PYNENC_KEY_PREFIX}:{app_id}:{prefix}"

    def invocation(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation:{invocation_id}"

    def task(self, task_id: str) -> str:
        return f"{self.prefix}task:{task_id}"

    def args(self, task_id: str, arg: str, val: str) -> str:
        return f"{self.prefix}task:{task_id}:arg:{arg}:val:{val}"

    def status(self, task_id: str, status: "InvocationStatus") -> str:
        return f"{self.prefix}task:{task_id}:status:{status}"

    def pending_timer(self, invocation_id: str) -> str:
        return f"{self.prefix}pending_timer:{invocation_id}"

    def previous_status(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_previous_status:{invocation_id}"

    def invocation_status(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_status:{invocation_id}"

    def invocation_retries(self, invocation_id: str) -> str:
        return f"{self.prefix}invocation_retries:{invocation_id}"

    def call(self, call_id: str) -> str:
        return f"{self.prefix}call:{call_id}"

    def call_to_invocation(self, call_id: str) -> str:
        return f"{self.prefix}call_to_invocation:{call_id}"

    def edge(self, call_id: str) -> str:
        return f"{self.prefix}edge:{call_id}"

    def waiting_for(self, invocation_id: str) -> str:
        return f"{self.prefix}waiting_for:{invocation_id}"

    def waited_by(self, invocation_id: str) -> str:
        return f"{self.prefix}waited_by:{invocation_id}"

    def all_waited(self) -> str:
        return f"{self.prefix}all_waited"

    def not_waiting(self) -> str:
        return f"{self.prefix}not_waiting"

    def history(self, invocation_id: str) -> str:
        return f"{self.prefix}history:{invocation_id}"

    def result(self, invocation_id: str) -> str:
        return f"{self.prefix}result:{invocation_id}"

    def exception(self, invocation_id: str) -> str:
        return f"{self.prefix}exception:{invocation_id}"

    def invocation_auto_purge(self) -> str:
        return f"{self.prefix}invocation_auto_purge"

    def default_queue(self) -> str:
        return f"{self.prefix}default_queue"

    def arg_cache(self, key: str) -> str:
        return f"{self.prefix}arg_key:{key}"

    def purge(self, client: redis.Redis) -> None:
        """
        Purges all keys with the given prefix in Redis.

        :param redis.Redis client: The Redis client.
        """
        pattern = f"{self.prefix}*"
        keys = list(client.scan_iter(pattern, count=1000))
        if keys:
            batch_size = 1000
            for i in range(0, len(keys), batch_size):
                batch = keys[i : i + batch_size]
                client.delete(*batch)

    def condition(self, condition_id: str) -> str:
        """Get key for storing a trigger condition."""
        return f"{self.prefix}condition:{condition_id}"

    def trigger(self, trigger_id: str) -> str:
        """Get key for storing a trigger definition."""
        return f"{self.prefix}trigger:{trigger_id}"

    def valid_condition(self, condition_id: str) -> str:
        """Get key for storing a valid condition."""
        return f"{self.prefix}valid_condition:{condition_id}"

    def task_triggers(self, task_id: str) -> str:
        """Get key for storing triggers associated with a task."""
        return f"{self.prefix}task_triggers:{task_id}"

    def condition_triggers(self, condition_id: str) -> str:
        """Get key for storing triggers that use a condition."""
        return f"{self.prefix}condition_triggers:{condition_id}"

    def event_channel(self) -> str:
        """Get channel name for publishing trigger events."""
        return f"{self.prefix}events"

    def cron_last_execution(self, condition_id: str) -> str:
        """
        Generate a key for storing the last execution time of a cron condition.

        :param condition_id: ID of the cron condition
        :return: Redis key string
        """
        return f"{self.prefix}cron_last_execution:{condition_id}"

    def source_task_conditions(self, task_id: str) -> str:
        """
        Generate key for source task to condition mapping.

        This key stores conditions that are sourced from a specific task.

        :param task_id: ID of the source task
        :return: Redis key for task's source conditions
        """
        return f"{self.prefix}source_task_conditions:{task_id}"

    def trigger_execution_claim(self, trigger_id: str, valid_condition_id: str) -> str:
        """
        Generate a key for a trigger execution claim.

        This key is used to atomically claim the right to execute a trigger
        for a specific valid condition across multiple workers.

        :param trigger_id: ID of the trigger definition
        :param valid_condition_id: ID of the valid condition
        :return: Redis key for the trigger execution claim
        """
        return (
            f"{self.prefix}:trigger:execution_claim:{trigger_id}:{valid_condition_id}"
        )

    def trigger_run_claim(self, trigger_run_id: str) -> str:
        """
        Generate a key for a trigger run claim.

        This key is used to atomically claim the right to execute a specific trigger run
        across multiple workers. A trigger run is a unique execution attempt for a
        trigger and its satisfied conditions.

        :param trigger_run_id: Unique ID for this trigger run
        :return: Redis key for the trigger run claim
        """
        return f"{self.prefix}:trigger:run_claim:{trigger_run_id}"

    def workflow_data_value(self, workflow_id: str, key: str) -> str:
        return f"{self.prefix}workflow:{workflow_id}:data:{key}"

    def workflow_deterministic_value(self, workflow_id: str, key: str) -> str:
        """
        Get key for storing a deterministic value for workflow operations.

        :param workflow_id: ID of the workflow
        :param key: Identifier for the deterministic value
        :return: Redis key for the deterministic value
        """
        return f"{self.prefix}workflow:{workflow_id}:det:{key}"

    @staticmethod
    def all_apps_info_key(app_id: str) -> str:
        """
        Get key for storing app information in the central registry.

        This uses a special prefix outside the normal app namespace
        to make discovery possible across all apps.

        :param app_id: The ID of the app
        :return: Redis key for app information
        """
        return f"{PYNENC_KEY_PREFIX}:{PYNENC_KEY_PREFIX}:apps_info:{app_id}"
