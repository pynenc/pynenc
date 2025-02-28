from typing import Generator
from unittest.mock import Mock, patch

import pytest

from pynenc.conf.config_redis import ConfigRedis
from pynenc.util.redis_client import get_redis_client


class TestRedisClient:
    @pytest.fixture
    def mock_redis(self) -> Generator:
        with patch("redis.Redis") as mock:
            yield mock

    @pytest.fixture
    def mock_redis_from_url(self) -> Generator:
        # Patch redis.Redis.from_url directly
        with patch("redis.Redis.from_url", autospec=True) as mock:
            yield mock

    def test_get_redis_client_with_url(self, mock_redis_from_url: Mock) -> None:
        """Test client creation with redis_url."""
        config = ConfigRedis()
        config.redis_url = "redis://localhost:6379/0"

        get_redis_client(config)

        mock_redis_from_url.assert_called_once_with(config.redis_url)

    def test_get_redis_client_with_credentials(self, mock_redis: Mock) -> None:
        """Test client creation with username and password."""
        config = ConfigRedis()
        config.redis_username = "user"
        config.redis_password = "pass"
        config.redis_host = "localhost"
        config.redis_port = 6379
        config.redis_db = 1

        get_redis_client(config)

        mock_redis.assert_called_once_with(
            host="localhost", port=6379, db=1, username="user", password="pass"
        )

    def test_get_redis_client_without_credentials(self, mock_redis: Mock) -> None:
        """Test client creation without credentials."""
        config = ConfigRedis()
        config.redis_username = ""
        config.redis_password = ""
        config.redis_host = "localhost"
        config.redis_port = 6379
        config.redis_db = 0

        get_redis_client(config)

        mock_redis.assert_called_once_with(
            host="localhost", port=6379, db=0, username=None, password=None
        )

    def test_get_redis_client_url_takes_precedence(
        self, mock_redis_from_url: Mock, mock_redis: Mock
    ) -> None:
        """Test that redis_url takes precedence over other settings."""
        config = ConfigRedis()
        config.redis_url = "redis://localhost:6379/0"
        config.redis_username = "user"
        config.redis_password = "pass"

        get_redis_client(config)

        mock_redis.from_url.assert_called_once_with(config.redis_url)
        mock_redis.assert_not_called()
