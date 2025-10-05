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
