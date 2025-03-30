import threading

import redis

from pynenc.conf.config_redis import ConfigRedis

_REDIS_POOLS = {}
_POOLS_LOCK = threading.RLock()


def get_redis_client(conf: ConfigRedis) -> redis.Redis:
    """Get a Redis client using connection pooling."""
    global _REDIS_POOLS

    if conf.redis_url:
        pool_key = conf.redis_url
    else:
        pool_key = f"{conf.redis_host}:{conf.redis_port}:{conf.redis_db}:{conf.redis_username or ''}:{conf.redis_password or ''}"

    with _POOLS_LOCK:
        if pool_key not in _REDIS_POOLS:
            max_connections = conf.redis_pool_max_connections
            if conf.redis_url:
                _REDIS_POOLS[pool_key] = redis.ConnectionPool.from_url(
                    conf.redis_url,
                    max_connections=max_connections,
                    socket_timeout=conf.socket_timeout,
                    socket_connect_timeout=conf.socket_connect_timeout,
                    socket_keepalive=True,
                    health_check_interval=conf.redis_pool_health_check_interval,
                    retry_on_timeout=True,
                )
            else:
                _REDIS_POOLS[pool_key] = redis.ConnectionPool(
                    host=conf.redis_host,
                    port=conf.redis_port,
                    db=conf.redis_db,
                    username=None if not conf.redis_username else conf.redis_username,
                    password=None if not conf.redis_password else conf.redis_password,
                    socket_timeout=conf.socket_timeout,
                    socket_connect_timeout=conf.socket_connect_timeout,
                    socket_keepalive=True,
                    health_check_interval=conf.redis_pool_health_check_interval,
                    max_connections=max_connections,
                    retry_on_timeout=True,
                )

    return redis.Redis(connection_pool=_REDIS_POOLS[pool_key])
