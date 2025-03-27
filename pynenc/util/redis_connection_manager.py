import logging
import time
from functools import wraps
from typing import Any, Callable, Optional, TypeVar, cast

import redis
from redis.exceptions import ConnectionError, TimeoutError

from pynenc.conf.config_redis import ConfigRedis

logger = logging.getLogger(__name__)

# Define a type variable for function return types
T = TypeVar("T")


def with_retry(max_retries: int = 3, retry_delay: float = 0.5) -> Callable:
    """
    Decorator to retry Redis operations with exponential backoff.

    :param int max_retries: Maximum number of retries before giving up
    :param float retry_delay: Initial delay between retries (doubles with each retry)
    :return: Decorated function with retry logic
    """

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Get the Redis client instance (first argument should be self)
            redis_mgr = args[0]

            retries = 0
            current_delay = retry_delay

            while True:
                try:
                    return func(*args, **kwargs)
                except (ConnectionError, TimeoutError) as e:
                    retries += 1
                    if retries > max_retries:
                        logger.error(
                            f"Max retries ({max_retries}) reached. Redis operation failed: {str(e)}"
                        )
                        raise

                    logger.warning(
                        f"Redis connection error: {str(e)}. "
                        f"Reconnecting (attempt {retries}/{max_retries}, delay: {current_delay:.2f}s)..."
                    )

                    # Try to reconnect
                    try:
                        redis_mgr.reconnect()
                    except Exception as conn_err:
                        logger.error(f"Failed to reconnect to Redis: {str(conn_err)}")

                    # Sleep with exponential backoff
                    time.sleep(current_delay)
                    current_delay *= 2  # Exponential backoff

        return wrapper

    return decorator


class RedisConnectionManager:
    """
    Manages Redis connections with automatic reconnection.

    This class wraps the Redis client and provides methods to execute commands
    with automatic reconnection if the connection is lost.
    """

    def __init__(self, conf: ConfigRedis) -> None:
        """
        Initialize the Redis connection manager.

        :param ConfigRedis conf: Redis configuration
        """
        self.conf = conf
        self._client: Optional[redis.Redis] = None
        self.connect()

    def connect(self) -> None:
        """
        Connect to Redis server using the provided configuration.

        :raises redis.ConnectionError: If connection fails
        """
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass

        if self.conf.redis_url:
            self._client = redis.Redis.from_url(
                self.conf.redis_url,
                socket_timeout=self.conf.socket_timeout,
                socket_connect_timeout=self.conf.socket_connect_timeout,
                socket_keepalive=True,
                health_check_interval=self.conf.health_check_interval,
            )
        else:
            self._client = redis.Redis(
                host=self.conf.redis_host,
                port=self.conf.redis_port,
                db=self.conf.redis_db,
                username=None
                if not self.conf.redis_username
                else self.conf.redis_username,
                password=None
                if not self.conf.redis_password
                else self.conf.redis_password,
                socket_timeout=self.conf.socket_timeout,
                socket_connect_timeout=self.conf.socket_connect_timeout,
                socket_keepalive=True,
                health_check_interval=self.conf.health_check_interval,
            )

        logger.debug(f"Connected to Redis at {self.get_connection_info()}")

    def reconnect(self) -> None:
        """
        Reconnect to Redis server.

        :raises redis.ConnectionError: If reconnection fails
        """
        logger.info("Reconnecting to Redis...")
        self.connect()

    def get_connection_info(self) -> str:
        """
        Get a string representation of the current connection.

        :return: String describing the Redis connection
        :rtype: str
        """
        if self.conf.redis_url:
            return self.conf.redis_url
        return f"{self.conf.redis_host}:{self.conf.redis_port}/{self.conf.redis_db}"

    @property
    def client(self) -> redis.Redis:
        """
        Get the Redis client instance.

        :return: The Redis client
        :rtype: redis.Redis
        :raises redis.ConnectionError: If connection fails
        """
        if not self._client:
            self.connect()
        return cast(redis.Redis, self._client)

    @with_retry(max_retries=3)
    def execute(self, method_name: str, *args: Any, **kwargs: Any) -> Any:
        """
        Execute a Redis command with automatic reconnection.

        :param str method_name: Name of the Redis client method to call
        :param args: Positional arguments to pass to the method
        :param kwargs: Keyword arguments to pass to the method
        :return: The result of the Redis command
        :raises redis.ConnectionError: If all reconnection attempts fail
        """
        method = getattr(self.client, method_name)
        return method(*args, **kwargs)


def get_redis_connection_manager(conf: ConfigRedis) -> RedisConnectionManager:
    """
    Create a Redis connection manager instance.

    :param ConfigRedis conf: Redis configuration
    :return: A Redis connection manager
    :rtype: RedisConnectionManager
    :raises redis.ConnectionError: If initial connection fails
    """
    return RedisConnectionManager(conf)
