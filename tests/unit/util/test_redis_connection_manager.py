from unittest.mock import Mock, call, patch

import pytest
import redis

from pynenc.conf.config_redis import ConfigRedis
from pynenc.util.redis_connection_manager import RedisConnectionManager, with_retry


def test_lazy_initialization() -> None:
    """Test that Redis connection is lazily initialized."""
    # Mock the Redis class
    with patch("redis.Redis"):
        # Create config and manager but don't access client yet
        config = ConfigRedis()
        config.redis_host = "localhost"
        config.redis_port = 6379
        config.redis_db = 0

        # Patch the connect method to avoid immediate connection
        with patch.object(RedisConnectionManager, "connect") as mock_connect:
            # Initialize but make _client property None to test lazy initialization
            manager = RedisConnectionManager(config)
            manager._client = None

            # Reset the connect mock to avoid counting the init call
            mock_connect.reset_mock()

            # Verify connect hasn't been called yet
            mock_connect.assert_not_called()

            # Access the client property
            # Note: This will fail because we mocked connect but not client property
            # but that's okay for this test which just verifies connect is called
            try:
                _ = manager.client
            except AttributeError:
                pass

            # Now connect should have been called
            mock_connect.assert_called_once()


def test_connect_with_url() -> None:
    """Test connection with redis_url."""
    # Mock Redis.from_url
    with patch("redis.Redis.from_url") as mock_from_url:
        mock_instance = Mock()
        mock_from_url.return_value = mock_instance

        config = ConfigRedis()
        config.redis_url = "redis://localhost:6379/0"
        config.socket_timeout = 5.0
        config.socket_connect_timeout = 3.0
        config.health_check_interval = 30

        with patch.object(RedisConnectionManager, "__init__", return_value=None):
            manager = RedisConnectionManager(None)  # type: ignore
            manager.conf = config
            manager._client = None
            manager.connect()

        mock_from_url.assert_called_once_with(
            config.redis_url,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.socket_connect_timeout,
            socket_keepalive=True,
            health_check_interval=config.health_check_interval,
        )


def test_connect_with_credentials() -> None:
    """Test connection with username and password."""
    # Mock Redis
    with patch("redis.Redis") as mock_redis:
        mock_instance = Mock()
        mock_redis.return_value = mock_instance

        config = ConfigRedis()
        config.redis_username = "user"
        config.redis_password = "pass"
        config.redis_host = "localhost"
        config.redis_port = 6379
        config.redis_db = 1
        config.socket_timeout = 5.0
        config.socket_connect_timeout = 3.0
        config.health_check_interval = 30

        with patch.object(RedisConnectionManager, "__init__", return_value=None):
            manager = RedisConnectionManager(None)  # type: ignore
            manager.conf = config
            manager._client = None
            manager.connect()

        mock_redis.assert_called_once_with(
            host=config.redis_host,
            port=config.redis_port,
            db=config.redis_db,
            username=config.redis_username,
            password=config.redis_password,
            socket_timeout=config.socket_timeout,
            socket_connect_timeout=config.socket_connect_timeout,
            socket_keepalive=True,
            health_check_interval=config.health_check_interval,
        )


def test_reconnect() -> None:
    """Test reconnect method."""
    config = ConfigRedis()

    # Create the manager directly
    manager = RedisConnectionManager(config)

    # Now patch connect and test reconnect
    with patch.object(RedisConnectionManager, "connect") as mock_connect:
        # Reset mock to clear initial connection from __init__
        mock_connect.reset_mock()

        # Call reconnect
        manager.reconnect()

        # Verify connect was called
        mock_connect.assert_called_once()


def test_with_retry_decorator() -> None:
    """Test the with_retry decorator."""
    # Create a mock function that raises ConnectionError twice then succeeds
    mock_func = Mock(
        side_effect=[
            redis.exceptions.ConnectionError("Connection failed"),
            redis.exceptions.ConnectionError("Connection failed again"),
            "success",
        ]
    )

    # Create a mock manager
    mock_manager = Mock(spec=RedisConnectionManager)
    mock_manager.reconnect = Mock()

    # Apply the decorator to our test function
    @with_retry(max_retries=3, retry_delay=0.01)
    def test_func(self: Mock, arg1: str, arg2: str | None = None) -> str:
        return mock_func(arg1, arg2)

    # Call the decorated function
    with patch("time.sleep") as mock_sleep:  # Patch sleep to avoid delays in tests
        result = test_func(mock_manager, "test", arg2="value")

    # Verify the function was called three times
    assert mock_func.call_count == 3
    mock_func.assert_has_calls(
        [call("test", "value"), call("test", "value"), call("test", "value")]
    )

    # Verify reconnect was called twice (for the two failures)
    assert mock_manager.reconnect.call_count == 2

    # Verify sleep was called twice (exponential backoff)
    assert mock_sleep.call_count == 2
    mock_sleep.assert_has_calls(
        [call(0.01), call(0.02)]  # First delay  # Second delay (doubled)
    )

    # Verify the final result is correct
    assert result == "success"


def test_with_retry_max_retries_exceeded() -> None:
    """Test that with_retry raises exception after max retries."""
    # Create a mock function that always raises ConnectionError
    mock_func = Mock(side_effect=redis.exceptions.ConnectionError("Connection failed"))

    # Create a mock manager
    mock_manager = Mock(spec=RedisConnectionManager)
    mock_manager.reconnect = Mock()

    # Apply the decorator to our test function
    @with_retry(max_retries=2, retry_delay=0.01)
    def test_func(self: Mock, arg1: str) -> str:
        return mock_func(arg1)

    # Call the decorated function
    with patch("time.sleep"), pytest.raises(redis.exceptions.ConnectionError):
        test_func(mock_manager, "test")

    # Verify the function was called three times (initial + 2 retries)
    assert mock_func.call_count == 3

    # Verify reconnect was called twice (for the two failures)
    assert mock_manager.reconnect.call_count == 2


def test_execute_method() -> None:
    """Test the execute method."""
    # Create a mock client
    mock_client = Mock()
    mock_client.get.return_value = "test_value"

    # Create a connection manager with the mock client
    with patch.object(RedisConnectionManager, "__init__", return_value=None):
        manager = RedisConnectionManager(None)  # type: ignore
        manager._client = mock_client

    # Bind the execute method
    manager.execute = RedisConnectionManager.execute.__get__(manager)

    # Call execute
    with patch.object(
        RedisConnectionManager, "client", property(lambda self: mock_client)
    ):
        result = manager.execute("get", "test_key")

    # Verify the client method was called
    mock_client.get.assert_called_once_with("test_key")

    # Verify the result
    assert result == "test_value"


def test_client_property_initializes_connection() -> None:
    """Test that accessing the client property initializes the connection if needed."""
    # SKIP CONSTRUCTOR: Create a direct instance and set attributes manually
    with patch.object(RedisConnectionManager, "__init__", return_value=None):
        manager = RedisConnectionManager(None)  # type: ignore
        manager.conf = ConfigRedis()
        manager._client = None  # Ensure disconnected state

        # Now patch connect to track calls
        with patch.object(manager, "connect") as mock_connect:
            # Set up connect to set _client when called
            mock_connect.side_effect = lambda: setattr(manager, "_client", Mock())

            # Access the client property
            client = manager.client

            # Verify connect was called
            mock_connect.assert_called_once()

            # Verify client property returned the right object
            assert client is manager._client


def test_handle_connection_error() -> None:
    """Test handling of connection errors."""
    # Create a simple mock Redis client
    mock_client = Mock()
    mock_client.get.side_effect = [
        redis.exceptions.ConnectionError("Connection error"),
        "success",  # Second call succeeds
    ]

    # Create a test function that will use our with_retry decorator
    @with_retry(max_retries=3, retry_delay=0.01)
    def test_func(self: Mock, cmd: str, key: str) -> str:
        return getattr(mock_client, cmd)(key)

    # Create a fake manager instance to pass to the retry decorator
    mock_manager = Mock(spec=RedisConnectionManager)
    mock_manager.reconnect = Mock()

    # Call the function with retry
    with patch("time.sleep"):  # Avoid actual delay
        result = test_func(mock_manager, "get", "test_key")

    # Verify the result is correct
    assert result == "success"

    # Verify reconnect was called after the connection error
    mock_manager.reconnect.assert_called_once()

    # Verify the Redis method was called twice
    assert mock_client.get.call_count == 2
