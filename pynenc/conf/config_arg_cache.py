from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_redis import ConfigRedis


class ConfigArgCache(ConfigPynencBase):
    """
    Configuration for the argument cache system.

    :cvar ConfigField[int] min_size_to_cache:
        Minimum string length (in characters) required to cache an argument.
        Arguments smaller than this size will be passed directly.
        Default is 1024 characters (roughly 1KB), as caching overhead
        isn't worth it for smaller values.

    :cvar ConfigField[int] local_cache_size:
        Maximum number of items to cache locally.
    """

    min_size_to_cache = ConfigField(1024)  # 1KB
    local_cache_size = ConfigField(1024)


class ConfigArgCacheRedis(ConfigArgCache, ConfigRedis):
    """Specific Configuration for the Redis Argument Cache"""
