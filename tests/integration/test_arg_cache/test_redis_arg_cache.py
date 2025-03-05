import json
from time import sleep
from typing import cast

import pytest

from pynenc.arg_cache.redis_arg_cache import RedisArgCache
from tests import util
from tests.conftest import MockPynenc


@pytest.fixture
def app() -> MockPynenc:
    """Create a test app with RedisArgCache."""
    app = MockPynenc(app_id="int-redis-arg-cache" + util.get_unique_id())
    app.arg_cache = RedisArgCache(app)  # type: ignore
    app.purge()
    return app


def get_redis_cache(app: MockPynenc) -> RedisArgCache:
    """
    Get the Redis arg cache instance with proper typing.

    :param MockPynenc app: The application instance
    :return: The Redis arg cache instance
    :raises TypeError: If the app's arg cache is not a RedisArgCache
    """
    if not isinstance(app.arg_cache, RedisArgCache):
        raise TypeError("App's arg cache is not a RedisArgCache")
    return cast(RedisArgCache, app.arg_cache)


def test_redis_storage(app: MockPynenc) -> None:
    """Test that values are properly stored in Redis."""
    redis_cache = get_redis_cache(app)
    test_value = "x" * (redis_cache.conf.min_size_to_cache + 100)
    key = redis_cache.serialize(test_value)

    # Small delay to ensure Redis operations complete
    sleep(0.1)

    # Verify value is in Redis using raw Redis client
    redis_key = redis_cache.key.arg_cache(key)
    raw_value = redis_cache.client.get(redis_key)
    assert raw_value is not None
    # Redis stores JSON-serialized value
    assert json.loads(raw_value.decode()) == test_value


def test_redis_purge(app: MockPynenc) -> None:
    """Test that purge removes values from Redis."""
    redis_cache = get_redis_cache(app)
    test_value = "x" * (redis_cache.conf.min_size_to_cache + 100)
    key = redis_cache.serialize(test_value)
    redis_key = redis_cache.key.arg_cache(key)

    # Verify value is in Redis
    assert redis_cache.client.exists(redis_key)


def test_redis_retrieve(app: MockPynenc) -> None:
    """Test retrieving values from Redis."""
    redis_cache = get_redis_cache(app)
    test_value = "x" * (redis_cache.conf.min_size_to_cache + 100)

    # Store value in Redis
    key = redis_cache.serialize(test_value)
    sleep(0.1)  # Ensure storage completes

    # Test direct retrieval using _retrieve
    retrieved_value = redis_cache._retrieve(key)
    # Redis returns JSON-serialized value
    assert json.loads(retrieved_value) == test_value

    # Test retrieval through deserialize (handles JSON deserialization)
    deserialized = redis_cache.deserialize(key)
    assert deserialized == test_value


def test_redis_retrieve_missing(app: MockPynenc) -> None:
    """Test retrieving non-existent values from Redis."""
    redis_cache = get_redis_cache(app)
    non_existent_key = f"{redis_cache.key.prefix}:nonexistent"

    # Should raise KeyError for missing keys
    with pytest.raises(KeyError, match="Cache key not found"):
        redis_cache._retrieve(non_existent_key)
