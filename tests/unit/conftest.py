from collections.abc import Generator
from unittest.mock import MagicMock, patch

import pytest

# Note: When adding new external dependencies (databases, message queues, etc.),
# add appropriate mocks here to prevent connections during tests.


@pytest.fixture(autouse=True)
def mock_redis_client() -> Generator["MagicMock", None, None]:
    """
    Mock Redis client to prevent Redis connections in tests.

    :yield: The mocked Redis client
    """
    mock_client = MagicMock()
    pipeline_mock = MagicMock()
    pipeline_mock.execute.return_value = []
    mock_client.pipeline.return_value = pipeline_mock
    with patch("redis.Redis", return_value=mock_client) as redis_mock:
        with patch("redis.cluster.RedisCluster", return_value=mock_client):
            yield redis_mock
