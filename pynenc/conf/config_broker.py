from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigBroker(ConfigPynencBase):
    """Main config of the broker components

    :cvar float queue_timeout_sec:
        Maximum time in seconds to block waiting for messages (0.1 default).
    """

    queue_timeout_sec = ConfigField(0.1)


class ConfigBrokerSQLite(ConfigBroker, ConfigSQLite):
    """Configuration for SQLite-based Broker component.

    Combines broker-specific settings with SQLite configuration.
    """
