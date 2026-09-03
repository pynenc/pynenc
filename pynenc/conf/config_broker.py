from __future__ import annotations

import fnmatch
import math
import re
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite
from pynenc.exceptions import ConfigError

DEFAULT_QUEUE = "default"
QUEUE_NAME_PATTERN = re.compile(r"^[A-Za-z0-9_.-]+$")
MIN_PRIORITY = -100.0
MAX_PRIORITY = 100.0
DEFAULT_PRIORITY = 0.0
T = TypeVar("T")


@dataclass(frozen=True)
class BrokerPriorityRule:
    """Priority rule matched against task ids using shell-style wildcards."""

    task_id: str
    priority: float


def validate_queue_name(queue_name: str) -> str:
    """Validate and return a portable broker queue name."""
    if not isinstance(queue_name, str):
        raise ConfigError(
            f"Invalid queue name {queue_name!r}. Queue names must match [A-Za-z0-9_.-]+"
        )
    if not QUEUE_NAME_PATTERN.fullmatch(queue_name):
        raise ConfigError(
            f"Invalid queue name {queue_name!r}. Queue names must match [A-Za-z0-9_.-]+"
        )
    return queue_name


def validate_priority(
    priority: Any,
    *,
    label: str = "Priority",
) -> float:
    """Validate and return a priority within Pynenc's public priority range."""
    valid_range = f"a float between {MIN_PRIORITY} and {MAX_PRIORITY}"
    try:
        result = float(priority)
    except (TypeError, ValueError) as exc:
        raise ConfigError(f"{label} must be {valid_range}") from exc
    if not math.isfinite(result) or not MIN_PRIORITY <= result <= MAX_PRIORITY:
        raise ConfigError(f"{label} must be {valid_range}")
    return result


def _sequence_from_config(
    value: Any, *, none_as_empty: bool = False
) -> tuple[Any, ...]:
    match value:
        case None if none_as_empty:
            return ()
        case str():
            return tuple(part.strip() for part in value.split(",") if part.strip())
        case Mapping() | bytes() | bytearray():
            pass
        case Iterable():
            return tuple(value)
    raise ConfigError(f"Expected a sequence or comma-separated string, got {value!r}")


def _queue_names_from_config(
    value: Any,
    *,
    allow_empty: bool = False,
    include_default: bool = False,
) -> tuple[str, ...]:
    queue_names = tuple(str(name) for name in _sequence_from_config(value))
    if not queue_names:
        if allow_empty:
            return ()
        raise ConfigError("Queue selection cannot be empty")
    if include_default and DEFAULT_QUEUE not in queue_names:
        queue_names = (DEFAULT_QUEUE, *queue_names)
    for queue_name in queue_names:
        validate_queue_name(queue_name)
    if len(set(queue_names)) != len(queue_names):
        raise ConfigError(f"Duplicate queue names: {queue_names!r}")
    return queue_names


def queue_names_config_mapper(value: Any, expected_type: type[T]) -> T:
    """Normalize broker queue config values to validated names."""
    queue_names = _queue_names_from_config(value, include_default=True)
    return cast(T, queue_names)


def runner_queue_names_config_mapper(value: Any, expected_type: type[T]) -> T:
    """Normalize runner queue config values to explicit queue names."""
    queue_names = _queue_names_from_config(value, allow_empty=True)
    return cast(T, queue_names)


def _priority_rule_from_mapping(raw_rule: Mapping[str, Any]) -> BrokerPriorityRule:
    try:
        task_id = str(raw_rule["task_id"])
        priority = validate_priority(
            raw_rule["priority"], label=f"Broker priority for {task_id!r}"
        )
    except KeyError as exc:
        raise ConfigError(
            "Broker priority rules require 'task_id' and 'priority'"
        ) from exc
    return BrokerPriorityRule(task_id=task_id, priority=priority)


def _validate_priority_rule(rule: BrokerPriorityRule) -> BrokerPriorityRule:
    if not rule.task_id:
        raise ConfigError("Broker priority rule task_id cannot be empty")
    return BrokerPriorityRule(
        task_id=rule.task_id,
        priority=validate_priority(
            rule.priority, label=f"Broker priority for {rule.task_id!r}"
        ),
    )


def priority_rules_mapper(value: Any, expected_type: type[T]) -> T:
    """Normalize broker priority rules to typed BrokerPriorityRule objects."""
    rules: list[BrokerPriorityRule] = []
    for raw_rule in _sequence_from_config(value, none_as_empty=True):
        match raw_rule:
            case BrokerPriorityRule():
                rule = raw_rule
            case Mapping():
                rule = _priority_rule_from_mapping(raw_rule)
            case _:
                raise ConfigError(
                    "Broker priority rules must be BrokerPriorityRule instances "
                    f"or mappings, got {raw_rule!r}"
                )
        rules.append(_validate_priority_rule(rule))
    return cast(T, tuple(rules))


class ConfigBroker(ConfigPynencBase):
    """Main configuration shared by broker components.

    :cvar ConfigField[float] queue_timeout_sec:
        Maximum time in seconds to block waiting for messages (0.1 default).

    :cvar ConfigField[tuple[str, ...]] queues:
        Logical queues accepted by the broker. The default queue is always
        included and queue names must be portable across broker backends.

    :cvar ConfigField[tuple[BrokerPriorityRule, ...]] priority_rules:
        Task-id patterns that override task priority. The highest matching
        priority wins when multiple rules match.

    :cvar ConfigField[bool] warn_on_queue_mismatch:
        Log a warning when runners do not consume every configured broker queue.

    :cvar ConfigField[bool] raise_on_queue_mismatch:
        Raise a configuration error instead of warning when runners do not
        consume every configured broker queue.
    """

    queue_timeout_sec = ConfigField(0.1)
    queues: ConfigField[tuple[str, ...]] = ConfigField(
        (DEFAULT_QUEUE,), mapper=queue_names_config_mapper
    )
    priority_rules: ConfigField[tuple[BrokerPriorityRule, ...]] = ConfigField(
        (), mapper=priority_rules_mapper
    )
    warn_on_queue_mismatch = ConfigField(True)
    raise_on_queue_mismatch = ConfigField(False)

    def get_priority_for_task(
        self, task_id_key: str, task_priority: float = DEFAULT_PRIORITY
    ) -> float:
        """Apply matching broker priority rules to a task priority."""
        matching_priorities = [
            rule.priority
            for rule in self.priority_rules
            if fnmatch.fnmatchcase(task_id_key, rule.task_id)
        ]
        if matching_priorities:
            return max(matching_priorities)
        return validate_priority(task_priority, label="Task priority")


class ConfigBrokerSQLite(ConfigBroker, ConfigSQLite):
    """Configuration for SQLite-based Broker component.

    Combines broker-specific settings with SQLite configuration.
    """
