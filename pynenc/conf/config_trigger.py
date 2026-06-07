from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigTrigger(ConfigPynencBase):
    """
    Configuration for the Trigger component.

    :cvar ConfigField[int] scheduler_interval_seconds:
        Interval in seconds for the scheduler to check for time-based triggers.
        Default is 60 seconds (1 minute).

    :cvar ConfigField[bool] enable_scheduler:
        Whether to enable the scheduler for time-based triggers.
        Default is True.

    :cvar ConfigField[int] max_events_batch_size:
        Maximum number of events to process in a single batch.
        Default is 100.

    :cvar ConfigField[int] event_retention_days:
        Number of days to retain event monitoring records.
        Used as the age threshold by ``BaseTrigger.auto_purge_events``.
        Default is 7 days.

    :cvar ConfigField[bool] event_auto_purge_enabled:
        Whether ``BaseTrigger.auto_purge_events`` performs deletions.
        When False, the wrapper short-circuits without calling the backend
        purge implementation. Default is True.

    :cvar ConfigField[int] event_max_records:
        Capacity-based retention for event monitoring records. When greater
        than zero, only the most recent N event records are kept across all
        codes. ``0`` disables the capacity check. Default is 0.

    :cvar ConfigField[int] trigger_run_max_records:
        Capacity-based retention for trigger-run monitoring records. ``0``
        disables the capacity check. Default is 0.

    """

    scheduler_interval_seconds = ConfigField(60)
    enable_scheduler = ConfigField(True)
    max_events_batch_size = ConfigField(100)
    event_retention_days = ConfigField(7)
    event_auto_purge_enabled = ConfigField(True)
    event_max_records = ConfigField(0)
    trigger_run_max_records = ConfigField(0)


class ConfigTriggerSQLite(ConfigTrigger, ConfigSQLite):
    """SQLite-specific configuration for the Trigger component."""
