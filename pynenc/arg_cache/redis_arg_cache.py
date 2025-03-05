from functools import cached_property
from typing import TYPE_CHECKING

from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.conf.config_arg_cache import ConfigArgCacheRedis
from pynenc.util.redis_client import get_redis_client
from pynenc.util.redis_keys import Key

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class RedisArgCache(BaseArgCache):
    """
    Redis-based implementation of argument caching.

    Stores serialized arguments in Redis for distributed access.
    Suitable for production use as cache is shared across all processes.
    """

    def __init__(self, app: "Pynenc") -> None:
        super().__init__(app)
        self.client = get_redis_client(self.conf)
        self.key = Key(app.app_id, "arg_cache")

    @cached_property
    def conf(self) -> ConfigArgCacheRedis:
        """Get Redis-specific configuration."""
        return ConfigArgCacheRedis(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    def _store(self, key: str, value: str) -> None:
        """
        Store a value in Redis.

        :param str key: The cache key
        :param str value: The serialized value to cache
        """
        self.client.set(self.key.arg_cache(key), value)

    def _retrieve(self, key: str) -> str:
        """
        Retrieve a value from Redis.

        :param str key: The cache key
        :return: The cached serialized value
        :raises KeyError: If key not found in cache
        """
        if value := self.client.get(self.key.arg_cache(key)):
            return value.decode()
        raise KeyError(f"Cache key not found: {key}")

    def _purge(self) -> None:
        """Clear all cached arguments from Redis."""
        self.key.purge(self.client)
