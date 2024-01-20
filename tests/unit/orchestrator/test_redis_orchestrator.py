from unittest.mock import MagicMock, Mock

import pytest

from pynenc.orchestrator.redis_orchestrator import (
    PendingInvocationLockError,
    TaskRedisCache,
)
from tests.conftest import MockPynenc


def test_set_pending_status_lock_error() -> None:
    # Mock the Redis client and lock
    mock_redis_client = Mock()
    mock_lock = MagicMock()
    mock_lock.acquire.side_effect = [False]  # First call to acquire returns False

    mock_redis_client.lock.return_value = mock_lock

    # Setup the TaskRedisCache with the mocked client
    app = MockPynenc()
    cache = TaskRedisCache(app, mock_redis_client)

    # Mock a DistributedInvocation
    mock_invocation = Mock()
    mock_invocation.invocation_id = "test_invocation_id"

    # Test
    with pytest.raises(PendingInvocationLockError):
        cache.set_pending_status(mock_invocation)

    # Assert the lock was attempted to be acquired and then released
    mock_lock.acquire.assert_called_once_with(blocking=True)
    mock_lock.release.assert_not_called()
