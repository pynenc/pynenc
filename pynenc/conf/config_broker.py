from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase


class ConfigBroker(ConfigPynencBase):
    """Main config of the boker components

    :cvar float queue_timeout_sec:
        Maximum time in seconds to block waiting for messages (0.1 default).
    """

    queue_timeout_sec = ConfigField(0.1)
