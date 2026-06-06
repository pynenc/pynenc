"""
Validation for trigger configuration.

Runs when ``BaseTrigger.conf`` is cached for the first time. Raises
TriggerConfigError only when the configuration can prevent trigger processing or
purge trigger monitoring records too aggressively.
"""

import warnings
from typing import TYPE_CHECKING

from pynenc.exceptions import ConfigError

if TYPE_CHECKING:
    from pynenc.conf.config_trigger import ConfigTrigger


class TriggerConfigError(ConfigError):
    """Raised when trigger configuration prevents trigger processing."""


def validate_trigger_config(conf: "ConfigTrigger") -> None:
    """
    Raise TriggerConfigError if trigger configuration cannot run.

    Hard failures:
    - event_retention_days < 1: auto-purge can remove current event records

    Warnings:
    - scheduler_interval_seconds <= 0: invalid if the scheduler starts using it
    - event_max_records == 1: composite event-trigger monitoring is fragile
    - trigger_run_max_records == 1: only one trigger-run record is retained
    """
    if not conf.enable_scheduler:
        return

    if conf.event_retention_days < 1:
        raise TriggerConfigError(
            f"event_retention_days must be >= 1; {conf.event_retention_days=}"
        )

    if conf.scheduler_interval_seconds <= 0:
        warnings.warn(
            "trigger: scheduler_interval_seconds <= 0; "
            f"{conf.scheduler_interval_seconds=}; "
            "time-based scheduling may not make progress if this config is used",
            UserWarning,
            stacklevel=2,
        )

    if conf.event_auto_purge_enabled and conf.event_max_records == 1:
        warnings.warn(
            "trigger: event_max_records keeps only one event record; "
            f"{conf.event_max_records=}; "
            "multi-event trigger-run monitoring may be purged aggressively",
            UserWarning,
            stacklevel=2,
        )

    if conf.event_auto_purge_enabled and conf.trigger_run_max_records == 1:
        warnings.warn(
            "trigger: trigger_run_max_records keeps only one trigger-run record; "
            f"{conf.trigger_run_max_records=}; "
            "trigger-run monitoring history may be purged aggressively",
            UserWarning,
            stacklevel=2,
        )
