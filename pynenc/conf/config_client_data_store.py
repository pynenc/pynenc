"""
Configuration for the ClientDataStore system.

Controls serialization caching, external storage thresholds, and local
cache sizes for client-provided data (arguments, results, exceptions).

Key components:
- ConfigClientDataStore: Base configuration for all client data store implementations
- ConfigClientDataStoreSQLite: SQLite-specific configuration
"""

from cistell import ConfigField

from pynenc.conf.config_base import ConfigPynencBase
from pynenc.conf.config_sqlite import ConfigSQLite


class ConfigClientDataStore(ConfigPynencBase):
    """
    Configuration for the client data store system.

    Controls when serialized values are stored externally vs returned inline,
    local caching behavior, and size monitoring thresholds.

    :cvar ConfigField[bool] disable_client_data_store: bool
        If True, bypass all client data store functionality, returning serialized data directly.
        Useful for testing or when external storage isn't needed. Default False.

    :cvar ConfigField[int] min_size_to_cache:
        Minimum serialized string length (characters) to store externally.
        Values smaller than this are returned inline as serialized strings.
        Default 1024 (~1KB) — caching overhead isn't worth it for smaller values.

    :cvar ConfigField[int] max_size_to_cache:
        Maximum serialized string length (characters) to store externally.
        Values larger than this will be stored in chunks.
        Set to 0 to disable the limit (no maximum). Default 0.
        Backend implementations may override based on storage constraints.

    :cvar ConfigField[int] local_cache_size:
        Maximum entries per local LRU cache tier. Default 1024.

    :cvar ConfigField[int] warn_threshold:
        Log a warning when any single value exceeds this size in bytes.
        Helps developers identify unexpectedly large arguments/results.
        Default 10MB (10_485_760 bytes).

    :cvar ConfigField[bool] compression_enabled:
        Enable compression for externally stored values. Default False.
        Future feature — reserved for zstd/lz4 compression support.
    """

    disable_client_data_store: ConfigField[bool] = ConfigField(False)
    min_size_to_cache: ConfigField[int] = ConfigField(1024)
    max_size_to_cache: ConfigField[int] = ConfigField(0)
    local_cache_size: ConfigField[int] = ConfigField(1024)
    warn_threshold: ConfigField[int] = ConfigField(10_485_760)
    compression_enabled: ConfigField[bool] = ConfigField(False)


class ConfigClientDataStoreSQLite(ConfigClientDataStore, ConfigSQLite):
    """SQLite-based client data store configuration."""
