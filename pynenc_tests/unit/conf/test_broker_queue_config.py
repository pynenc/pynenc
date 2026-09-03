import pytest

from pynenc.conf.config_broker import (
    DEFAULT_PRIORITY,
    MAX_PRIORITY,
    MIN_PRIORITY,
    BrokerPriorityRule,
    ConfigBroker,
    validate_queue_name,
)
from pynenc.conf.config_runner import ConfigRunner, QueueSelectionStrategy
from pynenc.exceptions import ConfigError
from pynenc.conf.validate_broker import validate_runner_broker_queues


def test_broker_queues_default_to_default_queue() -> None:
    conf = ConfigBroker(config_values={})

    assert conf.queues == ("default",)


def test_broker_queue_mapper_adds_default_and_validates_names() -> None:
    conf = ConfigBroker(config_values={"queues": "payments,reports"})

    assert conf.queues == ("default", "payments", "reports")

    with pytest.raises(ConfigError, match="Invalid queue name"):
        _ = ConfigBroker(config_values={"queues": ("bad queue",)}).queues


def test_runner_queues_accept_empty_default_or_explicit_names() -> None:
    assert ConfigRunner(config_values={}).queues == ()
    assert ConfigRunner(config_values={"queues": "payments,reports"}).queues == (
        "payments",
        "reports",
    )

    with pytest.raises(ConfigError, match="Invalid queue name"):
        _ = ConfigRunner(config_values={"queues": "*"}).queues


def test_broker_queue_config_rejects_wildcard_queue_names() -> None:
    with pytest.raises(ConfigError, match="Invalid queue name"):
        _ = ConfigBroker(config_values={"queues": ("*",)}).queues

    with pytest.raises(ConfigError, match="Invalid queue name"):
        _ = ConfigRunner(config_values={"queues": ("*",)}).queues


def test_validate_queue_name_rejects_non_string_values() -> None:
    with pytest.raises(ConfigError, match="Invalid queue name"):
        validate_queue_name(None)  # type: ignore[arg-type]


def test_runner_queue_selection_strategy_config() -> None:
    assert (
        ConfigRunner(config_values={}).queue_selection_strategy
        == QueueSelectionStrategy.ROUND_ROBIN
    )
    assert (
        ConfigRunner(
            config_values={"queue_selection_strategy": "ordered"}
        ).queue_selection_strategy
        == QueueSelectionStrategy.ORDERED
    )
    assert (
        ConfigRunner(
            config_values={"queue_selection_strategy": "random"}
        ).queue_selection_strategy
        == QueueSelectionStrategy.RANDOM
    )
    with pytest.raises(ConfigError, match="Invalid queue_selection_strategy"):
        _ = ConfigRunner(
            config_values={"queue_selection_strategy": "invalid"}
        ).queue_selection_strategy


def test_queue_mismatch_policy_defaults_to_warning_only() -> None:
    conf = ConfigBroker(config_values={})

    assert conf.warn_on_queue_mismatch is True
    assert conf.raise_on_queue_mismatch is False


def test_runner_queue_mismatch_policy_warns_or_raises() -> None:
    broker_conf = ConfigBroker(config_values={"queues": ("default",)})
    runner_conf = ConfigRunner(config_values={"queues": ("undeclared",)})

    with pytest.warns(UserWarning, match="not configured in broker.queues"):
        validate_runner_broker_queues(broker_conf, runner_conf)

    broker_conf.raise_on_queue_mismatch = True
    with pytest.raises(ConfigError, match="not configured in broker.queues"):
        validate_runner_broker_queues(broker_conf, runner_conf)


def test_priority_rules_use_highest_wildcard_match() -> None:
    conf = ConfigBroker(
        config_values={
            "priority_rules": (
                {"task_id": "billing.*", "priority": 20},
                BrokerPriorityRule(task_id="billing.urgent_*", priority=100.0),
            )
        }
    )

    assert conf.get_priority_for_task("billing.urgent_refund") == 100.0
    assert conf.get_priority_for_task("billing.invoice") == 20.0
    assert conf.get_priority_for_task("reports.daily") == DEFAULT_PRIORITY


def test_priority_rules_accept_portable_boundaries() -> None:
    conf = ConfigBroker(
        config_values={
            "priority_rules": (
                {"task_id": "low.*", "priority": MIN_PRIORITY},
                {"task_id": "high.*", "priority": MAX_PRIORITY},
            )
        }
    )

    assert conf.get_priority_for_task("low.task", 10.0) == MIN_PRIORITY
    assert conf.get_priority_for_task("high.task", 10.0) == MAX_PRIORITY
    assert conf.get_priority_for_task("other.task", 10.0) == 10.0


def test_priority_rules_reject_non_finite_values() -> None:
    with pytest.raises(ConfigError, match="between -100.0 and 100.0"):
        _ = ConfigBroker(
            config_values={
                "priority_rules": ({"task_id": "billing.*", "priority": "nan"},)
            }
        ).priority_rules


def test_priority_rules_reject_out_of_range_values() -> None:
    for priority in (MAX_PRIORITY + 0.1, MIN_PRIORITY - 0.1, float("-inf")):
        with pytest.raises(ConfigError, match="between -100.0 and 100.0"):
            _ = ConfigBroker(
                config_values={
                    "priority_rules": ({"task_id": "billing.*", "priority": priority},)
                }
            ).priority_rules
