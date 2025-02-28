import redis

from pynenc.conf.config_redis import ConfigRedis


def get_redis_client(conf: ConfigRedis) -> redis.Redis:
    """
    Creates a Redis client instance based on configuration.

    If redis_url is specified, creates client from URL.
    Otherwise, creates client using individual connection parameters.
    Empty username/password strings are treated as None.

    :param ConfigRedis conf: Redis configuration object
    :return: Configured Redis client instance
    :raises redis.ConnectionError: If connection fails
    """
    if conf.redis_url:
        return redis.Redis.from_url(conf.redis_url)

    return redis.Redis(
        host=conf.redis_host,
        port=conf.redis_port,
        db=conf.redis_db,
        username=None if not conf.redis_username else conf.redis_username,
        password=None if not conf.redis_password else conf.redis_password,
    )
