from typing import TYPE_CHECKING

import pytest

from pynenc.serializer.constants import ReservedKeys

if TYPE_CHECKING:
    from pynenc import Pynenc


def test_store_and_retrieve(app_instance: "Pynenc") -> None:
    """Test basic storage and retrieval of values."""
    test_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)
    key = app_instance.arg_cache.serialize(test_value)

    # Verify it's a cache key
    assert app_instance.arg_cache.is_cache_key(key)
    assert key.startswith(f"{ReservedKeys.ARG_CACHE.value}:")

    # Verify retrieval
    retrieved = app_instance.arg_cache.deserialize(key)
    assert retrieved == test_value


def test_cache_threshold(app_instance: "Pynenc") -> None:
    """Test that only values above threshold are cached."""
    small_value = "small"
    large_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)

    # Small value shouldn't be cached
    small_result = app_instance.arg_cache.serialize(small_value)
    assert not app_instance.arg_cache.is_cache_key(small_result)
    assert small_result == app_instance.serializer.serialize(small_value)

    # Large value should be cached
    large_result = app_instance.arg_cache.serialize(large_value)
    assert app_instance.arg_cache.is_cache_key(large_result)


def test_cache_reuse(app_instance: "Pynenc") -> None:
    """Test that identical values reuse cache entries."""
    test_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)

    # First serialization
    key1 = app_instance.arg_cache.serialize(test_value)

    # Second serialization of same value
    key2 = app_instance.arg_cache.serialize(test_value)

    # Should get same cache key
    assert key1 == key2


def test_disabled_cache(app_instance: "Pynenc") -> None:
    """Test that DisabledArgCache bypasses caching."""
    from pynenc.arg_cache.disabled_arg_cache import DisabledArgCache

    app_instance.arg_cache = DisabledArgCache(app_instance)  # type: ignore
    test_value = "x" * 1000000  # Large value

    # Should return serialized value directly
    result = app_instance.arg_cache.serialize(test_value)
    assert result == app_instance.serializer.serialize(test_value)
    assert not app_instance.arg_cache.is_cache_key(result)


def test_cache_purge(app_instance: "Pynenc") -> None:
    """Test cache purging functionality."""
    test_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)
    key = app_instance.arg_cache.serialize(test_value)

    # Verify value is cached
    assert app_instance.arg_cache.deserialize(key) == test_value

    # Purge cache
    app_instance.arg_cache.purge()

    # Verify value is no longer cached
    with pytest.raises(KeyError):
        app_instance.arg_cache.deserialize(key)


def test_disable_cache_flag(app_instance: "Pynenc") -> None:
    """Test that disable_cache flag prevents caching."""
    test_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)

    # Normal serialization
    key1 = app_instance.arg_cache.serialize(test_value)
    assert app_instance.arg_cache.is_cache_key(key1)

    # Disabled cache serialization
    result = app_instance.arg_cache.serialize(test_value, disable_cache=True)
    assert not app_instance.arg_cache.is_cache_key(result)
    assert result == app_instance.serializer.serialize(test_value)


def test_max_size_to_cache(app_instance: "Pynenc") -> None:
    """Test that values above max_size_to_cache are not cached."""
    # Set a max size limit
    original_max_size = app_instance.arg_cache.conf.max_size_to_cache
    app_instance.arg_cache.conf.max_size_to_cache = 5000

    try:
        # Value within limits should be cached
        medium_value = "x" * (app_instance.arg_cache.conf.min_size_to_cache + 100)
        medium_result = app_instance.arg_cache.serialize(medium_value)
        assert app_instance.arg_cache.is_cache_key(medium_result)

        # Value above max should not be cached
        large_value = "x" * 10000
        large_result = app_instance.arg_cache.serialize(large_value)
        assert not app_instance.arg_cache.is_cache_key(large_result)
        assert large_result == app_instance.serializer.serialize(large_value)
    finally:
        # Restore original value
        app_instance.arg_cache.conf.max_size_to_cache = original_max_size


def test_very_large_argument_handling(app_instance: "Pynenc") -> None:
    """
    Test that very large arguments are handled properly by the backend.

    This test uses a value large enough (~20MB serialized) to exceed common
    backend size limits (e.g., document size limits in NoSQL databases,
    VARCHAR limits in RDBMS implementations).

    If max_size_to_cache is configured, large arguments exceeding that limit
    should NOT be cached and should be returned as serialized values directly.

    If max_size_to_cache is 0 (no limit), the implementation will attempt to
    cache the value, which may FAIL if the backend has size constraints that
    haven't been configured properly. This forces implementations with size
    limits to set an appropriate max_size_to_cache value.
    """
    # Create a value large enough to exceed common backend limits
    # 20 million characters (~20MB) exceeds most default limits
    very_large_value = "x" * 20_000_000

    # This should not crash - either it gets cached (if limit allows) or
    # returns serialized value directly (if exceeds max_size_to_cache)
    result = app_instance.arg_cache.serialize(very_large_value)

    # Verify the value is usable regardless of whether it was cached
    if app_instance.arg_cache.is_cache_key(result):
        # If it was cached, we should be able to retrieve it
        retrieved = app_instance.arg_cache.deserialize(result)
        assert retrieved == very_large_value
    else:
        # If not cached, result should be the serialized value
        assert result == app_instance.serializer.serialize(very_large_value)
        # And we should be able to deserialize it directly
        deserialized = app_instance.serializer.deserialize(result)
        assert deserialized == very_large_value
