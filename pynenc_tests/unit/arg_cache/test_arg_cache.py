import pytest

from pynenc.conf.config_arg_cache import ConfigArgCache
from pynenc.serializer.constants import ReservedKeys
from pynenc_tests.conftest import MockArgCache, MockPynenc

mock_base_app = MockPynenc()


def test_base_arg_cache_conf() -> None:
    """Test that the arg cache configuration is properly loaded."""
    # Create an instance of BaseArgCache
    arg_cache = MockArgCache(app=mock_base_app)

    # Test the conf property
    conf = arg_cache.conf
    assert isinstance(conf, ConfigArgCache)
    assert conf.min_size_to_cache == 1024


def test_cache_large_argument() -> None:
    """Test that large arguments are properly cached."""
    arg_cache = MockArgCache(app=mock_base_app)

    # Create a large string that exceeds min_size_to_cache
    large_data = "x" * (arg_cache.conf.min_size_to_cache + 100)

    # First serialization should store in cache
    key = arg_cache.serialize(large_data)
    assert arg_cache.is_cache_key(key)

    # Check it was stored
    arg_cache._store_mock.assert_called_once()

    # Deserialize should retrieve from cache
    result = arg_cache.deserialize(key)
    assert result == large_data
    # No need to call _retrive as exists in data cache
    arg_cache._retrieve_mock.assert_not_called()


def test_skip_small_argument() -> None:
    """Test that small arguments bypass the cache."""
    arg_cache = MockArgCache(app=mock_base_app)

    # Create a small string below min_size_to_cache
    small_data = "small"

    # Should return serialized data directly
    result = arg_cache.serialize(small_data)
    assert not arg_cache.is_cache_key(result)

    # Check storage wasn't called
    arg_cache._store_mock.assert_not_called()


def test_disable_cache_flag() -> None:
    """Test that disable_cache flag bypasses caching regardless of size."""
    arg_cache = MockArgCache(app=mock_base_app)

    # Create a large string that would normally be cached
    large_data = "x" * (arg_cache.conf.min_size_to_cache + 100)

    # Serialize with cache disabled
    result = arg_cache.serialize(large_data, disable_cache=True)
    assert not arg_cache.is_cache_key(result)

    # Check storage wasn't called
    arg_cache._store_mock.assert_not_called()


def test_cache_key_format() -> None:
    """Test that cache keys are properly formatted."""
    arg_cache = MockArgCache(app=mock_base_app)
    test_data = "x" * arg_cache.conf.min_size_to_cache

    key = arg_cache.serialize(test_data)
    assert arg_cache.is_cache_key(key)
    assert ":" in key  # Should contain separator
    assert key.startswith(ReservedKeys.ARG_CACHE.value)  # Should have proper prefix


def test_purge() -> None:
    """Test that purge clears all caches."""
    arg_cache = MockArgCache(app=mock_base_app)

    # Add some items to cache
    large_data = "x" * arg_cache.conf.min_size_to_cache
    _ = arg_cache.serialize(large_data)

    # Purge should clear everything
    arg_cache.purge()

    # Check internal caches are cleared
    assert len(arg_cache._obj_id_cache) == 0
    assert len(arg_cache._hash_cache) == 0
    assert len(arg_cache._fingerprint_cache) == 0
    assert len(arg_cache._key_cache) == 0
    assert len(arg_cache._deserialized_cache) == 0

    # Check backend purge was called
    arg_cache._purge_mock.assert_called_once()


def test_cache_miss_handling() -> None:
    """Test handling of local cache misses. should call ._retrieve of subclass"""
    arg_cache = MockArgCache(app=mock_base_app)

    # Set up mock to raise KeyError
    arg_cache._retrieve_mock.side_effect = Exception("Abort on _retrieve call")

    # Try to deserialize non-existent key
    non_existent_key = f"{ReservedKeys.ARG_CACHE.value}:nonexistent"
    with pytest.raises(Exception, match="Abort on _retrieve call"):
        arg_cache.deserialize(non_existent_key)

    # Verify _retrieve was called with the key
    arg_cache._retrieve_mock.assert_called_once_with(non_existent_key)


def test_cache_key_passthrough() -> None:
    """Test that passing a cache key to serialize returns the same key without re-serializing."""
    arg_cache = MockArgCache(app=mock_base_app)

    # First create a cache key by serializing a large value
    large_data = "x" * (arg_cache.conf.min_size_to_cache + 100)
    cache_key = arg_cache.serialize(large_data)

    # Reset mock counters
    arg_cache._store_mock.reset_mock()

    # Now pass the cache key to serialize again
    result = arg_cache.serialize(cache_key)

    # The result should be the same cache key
    assert result == cache_key

    # Verify _store wasn't called - no new serialization
    arg_cache._store_mock.assert_not_called()

    # Also test that deserializing works correctly
    deserialized = arg_cache.deserialize(cache_key)
    assert deserialized == large_data


def test_cache_key_in_complex_structure() -> None:
    """Test that cache keys inside complex structures are preserved during serialization."""
    arg_cache = MockArgCache(app=mock_base_app)

    # First create some cache keys
    large_data1 = "x" * (arg_cache.conf.min_size_to_cache + 100)
    large_data2 = "y" * (arg_cache.conf.min_size_to_cache + 200)

    key1 = arg_cache.serialize(large_data1)
    key2 = arg_cache.serialize(large_data2)

    # Reset mocks
    arg_cache._store_mock.reset_mock()

    # Create a complex structure with cache keys
    complex_data = {
        "normal_key": "small_value",
        "cached_key1": key1,
        "nested": {"cached_key2": key2, "list_with_keys": [1, 2, key1, key2]},
    }

    # Serialize the complex structure
    complex_key = arg_cache.serialize(complex_data)

    # Deserialize and check structure
    result = arg_cache.deserialize(complex_key)

    # Verify the cache keys in the complex structure were preserved
    assert result["cached_key1"] == key1
    assert result["nested"]["cached_key2"] == key2
    assert result["nested"]["list_with_keys"][2] == key1
    assert result["nested"]["list_with_keys"][3] == key2

    # Deserialize the keys in the complex structure
    assert arg_cache.deserialize(result["cached_key1"]) == large_data1
    assert arg_cache.deserialize(result["nested"]["cached_key2"]) == large_data2
