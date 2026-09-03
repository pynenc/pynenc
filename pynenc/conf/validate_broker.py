"""Validation for broker configuration."""

import warnings
from typing import TYPE_CHECKING

from pynenc.exceptions import ConfigError


if TYPE_CHECKING:
    from pynenc.conf.config_broker import ConfigBroker
    from pynenc.conf.config_runner import ConfigRunner


def validate_runner_broker_queues(
    broker_conf: "ConfigBroker", runner_conf: "ConfigRunner"
) -> None:
    """Report runner queues that are not declared by the broker."""
    undeclared = tuple(
        queue for queue in runner_conf.queues if queue not in broker_conf.queues
    )
    if not undeclared:
        return
    message = f"Runner queues not configured in broker.queues: {undeclared!r}"
    if broker_conf.raise_on_queue_mismatch:
        raise ConfigError(message)
    if broker_conf.warn_on_queue_mismatch:
        warnings.warn(message, UserWarning, stacklevel=2)
