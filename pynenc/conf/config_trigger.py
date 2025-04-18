from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_redis import ConfigRedis


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
        Number of days to retain event history.
        Default is 7 days.
    """

    scheduler_interval_seconds = ConfigField(60)
    enable_scheduler = ConfigField(True)
    max_events_batch_size = ConfigField(100)
    event_retention_days = ConfigField(7)


class ConfigTriggerRedis(ConfigTrigger, ConfigRedis):
    """Specific Configuration for the Redis Trigger"""
