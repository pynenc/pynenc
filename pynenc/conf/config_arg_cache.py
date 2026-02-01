from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigArgCache(ConfigPynencBase):
    """
    Configuration for the argument cache system.

    :cvar ConfigField[int] min_size_to_cache:
        Minimum string length (in characters) required to cache an argument.
        Arguments smaller than this size will be passed directly.
        Default is 1024 characters (roughly 1KB), as caching overhead
        isn't worth it for smaller values.

    :cvar ConfigField[int] max_size_to_cache:
        Maximum string length (in characters) allowed to cache an argument.
        Arguments larger than this size will not be cached and will be
        serialized directly. Set to 0 to disable the limit (no maximum).
        Default is 0 (no limit).

        Backend implementations may need to set a specific limit based on
        their storage constraints (e.g., MongoDB has a 16MB document limit).

    :cvar ConfigField[int] local_cache_size:
        Maximum number of items to cache locally.
    """

    min_size_to_cache = ConfigField(1024)  # 1KB
    max_size_to_cache = ConfigField(0)  # 0 = no limit
    local_cache_size = ConfigField(1024)


class ConfigArgCacheSQLite(ConfigArgCache, ConfigSQLite):
    """SQLite-based argument cache configuration"""
