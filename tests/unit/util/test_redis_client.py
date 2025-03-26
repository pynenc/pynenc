from collections.abc import Generator
from unittest.mock import Mock, patch

import pytest

from pynenc.conf.config_redis import ConfigRedis
from pynenc.util.redis_client import get_redis_client
from pynenc.util.redis_connection_manager import RedisConnectionManager


@pytest.fixture
def mock_connection_manager() -> Generator:
    with patch("pynenc.util.redis_client.get_redis_connection_manager") as mock:
        # Setup mock connection manager with client property
        mock_manager = Mock(spec=RedisConnectionManager)
        # Don't use spec here - just create a simple mock
        mock_client = Mock(name="redis_client")
        mock_manager.client = mock_client
        mock.return_value = mock_manager
        yield mock, mock_manager, mock_client


def test_get_redis_client_uses_connection_manager(
    mock_connection_manager: tuple,
) -> None:
    """Test that get_redis_client uses the connection manager."""
    mock_get_manager, mock_manager, mock_client = mock_connection_manager
    config = ConfigRedis()

    # Call the function under test
    client = get_redis_client(config)

    # Verify the connection manager was created with the config
    mock_get_manager.assert_called_once_with(config)

    # Verify the client property was accessed
    assert client == mock_client
