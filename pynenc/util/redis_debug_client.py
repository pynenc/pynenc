import logging
import time
from functools import wraps
from typing import Any, Callable, TypeVar

import redis

logger = logging.getLogger(__name__)

# Define a type variable for function return types
T = TypeVar("T")


def log_redis_command(func: Callable[..., T]) -> Callable[..., T]:
    """Decorator to log Redis commands with timing information."""

    @wraps(func)
    def wrapper(*args: Any, **kwargs: Any) -> T:
        # Get function name for logging
        func_name = func.__name__

        # Format arguments for logging (truncate large values)
        def format_arg(arg: Any) -> str:
            str_arg = str(arg)
            if len(str_arg) > 100:
                return f"{str_arg[:100]}... [truncated, len={len(str_arg)}]"
            return str_arg

        formatted_args = [format_arg(arg) for arg in args[1:]]  # Skip self
        formatted_kwargs = {k: format_arg(v) for k, v in kwargs.items()}

        # Log command start
        log_prefix = "REDIS_CMD"
        logger.debug(
            f"{log_prefix} START: {func_name}({', '.join(formatted_args)}, {formatted_kwargs})"
        )

        # Execute command and time it
        start_time = time.time()
        try:
            result = func(*args, **kwargs)
            elapsed = time.time() - start_time

            # Format result for logging
            result_str = str(result)
            if len(result_str) > 100:
                result_str = f"{result_str[:100]}... [truncated, len={len(result_str)}]"

            # Log command completion with timing
            if elapsed > 0.1:  # Only log slow commands with INFO level
                logger.info(
                    f"{log_prefix} SLOW: {func_name} took {elapsed:.6f}s - Result: {result_str}"
                )
            else:
                logger.debug(
                    f"{log_prefix} END: {func_name} took {elapsed:.6f}s - Result: {result_str}"
                )

            return result

        except Exception as e:
            elapsed = time.time() - start_time
            logger.error(
                f"{log_prefix} ERROR: {func_name} failed after {elapsed:.6f}s - {type(e).__name__}: {e}"
            )
            raise

    return wrapper


class DebugRedisClient(redis.Redis):
    """Redis client that logs all commands with timing information."""

    def __getattribute__(self, name: str) -> Any:
        attr = super().__getattribute__(name)

        # Only wrap callable Redis commands, not internal methods or attributes
        if (
            callable(attr)
            and not name.startswith("_")
            and name
            not in ["execute_command", "parse_response", "connection_pool", "from_url"]
        ):
            return log_redis_command(attr)
        return attr


# Now modify RedisConnectionManager to use this client class
def patch_redis_connection_manager() -> None:
    """Patch the RedisConnectionManager to use the debug client."""
    from pynenc.util.redis_connection_manager import RedisConnectionManager

    original_connect = RedisConnectionManager.connect

    @wraps(original_connect)
    def patched_connect(self: RedisConnectionManager) -> None:
        """Patched version of connect that uses DebugRedisClient."""
        if self._client:
            try:
                self._client.close()
            except Exception:
                pass

        logger.info(
            f"REDIS_DEBUG: Creating new debug Redis client connection to {self.get_connection_info()}"
        )

        if self.conf.redis_url:
            pool = redis.ConnectionPool.from_url(
                self.conf.redis_url,
                socket_timeout=self.conf.socket_timeout,
                socket_connect_timeout=self.conf.socket_connect_timeout,
                socket_keepalive=True,
                health_check_interval=self.conf.health_check_interval,
            )
            self._client = DebugRedisClient(connection_pool=pool)
        else:
            pool = redis.ConnectionPool(
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
            self._client = DebugRedisClient(connection_pool=pool)

        logger.info(
            f"REDIS_DEBUG: Successfully connected to Redis at {self.get_connection_info()}"
        )

        # Ping to test connection
        try:
            ping_start = time.time()
            res = self._client.ping()
            ping_elapsed = time.time() - ping_start
            logger.info(
                f"REDIS_DEBUG: Redis ping response: {res} in {ping_elapsed:.6f}s"
            )
        except Exception as e:
            logger.error(f"REDIS_DEBUG: Redis ping failed: {e}")

    # Apply the patch
    RedisConnectionManager.connect = patched_connect  # type: ignore
    logger.info("REDIS_DEBUG: RedisConnectionManager patched with debugging client")


# Automatically apply the patch when this module is imported
patch_redis_connection_manager()
