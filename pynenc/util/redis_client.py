import redis

from pynenc.conf.config_redis import ConfigRedis
from pynenc.util.redis_connection_manager import get_redis_connection_manager


def get_redis_client(conf: ConfigRedis) -> redis.Redis:
    """
    Creates a Redis client instance based on configuration.

    If redis_url is specified, creates client from URL.
    Otherwise, creates client using individual connection parameters.
    Empty username/password strings are treated as None.
    Uses the connection manager to provide more robust connections
    with automatic reconnection capabilities.

    :param ConfigRedis conf: Redis configuration object
    :return: Configured Redis client instance
    :raises redis.ConnectionError: If connection fails
    """
    connection_manager = get_redis_connection_manager(conf)
    return connection_manager.client
