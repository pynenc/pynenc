from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigStateBackend(ConfigPynencBase):
    """Main config of the state backend components"""


class ConfigStateBackendRedis(ConfigStateBackend, ConfigRedis):
    """
    Specific Configuration for the Redis State Backend.

    :cvar ConfigField[int] pagination_batch_size:
        Number of items to retrieve in each batch when paginating through large
        collections of workflow runs or other state data.
    """

    pagination_batch_size = ConfigField(100)
