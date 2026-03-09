import pytest

from pynenc.conf.config_client_data_store import ConfigClientDataStore
from pynenc.serializer.constants import ReservedKeys
from pynenc_tests.conftest import MockClientDataStore, MockPynenc


@pytest.fixture
def mock_base_app() -> MockPynenc:
    """Fixture to provide a fresh MockPynenc instance for each test."""
    return MockPynenc()


def test_base_client_data_store_conf(mock_base_app: "MockPynenc") -> None:
    """Test that the client data store configuration is properly loaded."""
    # Create an instance of BaseClientDataStore
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Test the conf property
    conf = client_data_store.conf
    assert isinstance(conf, ConfigClientDataStore)
    assert conf.min_size_to_cache == 1024


def test_cache_large_argument(mock_base_app: "MockPynenc") -> None:
    """Test that large arguments are properly cached."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Create a large string that exceeds min_size_to_cache
    large_data = "x" * (client_data_store.conf.min_size_to_cache + 100)

    # First serialization should store in cache
    key = client_data_store.serialize(large_data)
    assert client_data_store.is_reference(key)
    # Check it was stored
    client_data_store._store.assert_called_once()

    # Deserialize should retrieve from cache
    result = client_data_store.deserialize(key)
    assert result == large_data
    # No need to call _retrive as exists in data cache
    client_data_store._retrieve.assert_not_called()


def test_skip_small_argument(mock_base_app: "MockPynenc") -> None:
    """Test that small arguments bypass the cache."""
    client_data_store = MockClientDataStore(app=mock_base_app)
    # Create a small string below min_size_to_cache
    small_data = "small"

    # Should return serialized data directly
    result = client_data_store.serialize(small_data)
    assert not client_data_store.is_reference(result)

    # Check storage wasn't called
    client_data_store._store.assert_not_called()


def test_disable_cache_flag(mock_base_app: "MockPynenc") -> None:
    """Test that disable_cache flag bypasses caching regardless of size."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Create a large string that would normally be cached
    large_data = "x" * (client_data_store.conf.min_size_to_cache + 100)

    # Serialize with cache disabled
    result = client_data_store.serialize(large_data, disable_cache=True)
    assert not client_data_store.is_reference(result)
    # Check storage wasn't called
    client_data_store._store.assert_not_called()


def test_cache_key_format(mock_base_app: "MockPynenc") -> None:
    """Test that cache keys are properly formatted."""
    client_data_store = MockClientDataStore(app=mock_base_app)
    test_data = "x" * client_data_store.conf.min_size_to_cache

    key = client_data_store.serialize(test_data)
    assert client_data_store.is_reference(key)
    assert ":" in key  # Should contain separator
    assert key.startswith(ReservedKeys.CLIENT_DATA.value)  # Should have proper prefix


def test_purge(mock_base_app: "MockPynenc") -> None:
    """Test that purge clears all caches."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Add some items to cache
    large_data = "x" * client_data_store.conf.min_size_to_cache
    _ = client_data_store.serialize(large_data)
    # Purge should clear everything
    client_data_store.purge()

    # Check internal caches are cleared
    assert len(client_data_store._deserialized_cache) == 0
    # Check backend purge was called
    client_data_store._purge.assert_called_once()


def test_cache_miss_handling(mock_base_app: "MockPynenc") -> None:
    """Test handling of local cache misses. should call ._retrieve of subclass"""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Set up mock to raise KeyError
    client_data_store._retrieve.side_effect = Exception("Abort on _retrieve call")

    # Try to deserialize non-existent key
    non_existent_key = f"{ReservedKeys.CLIENT_DATA.value}:nonexistent"
    with pytest.raises(Exception, match="Abort on _retrieve call"):
        client_data_store.deserialize(non_existent_key)

    # Verify _retrieve was called with the key
    client_data_store._retrieve.assert_called_once_with(non_existent_key)


def test_cache_key_passthrough(mock_base_app: "MockPynenc") -> None:
    """Test that passing a cache key to serialize returns the same key without re-serializing."""
    client_data_store = MockClientDataStore(app=mock_base_app)
    # First create a cache key by serializing a large value
    large_data = "x" * (client_data_store.conf.min_size_to_cache + 100)
    cache_key = client_data_store.serialize(large_data)

    # Reset mock counters
    client_data_store._store.reset_mock()

    # Now pass the cache key to serialize again
    result = client_data_store.serialize(cache_key)

    # The result should be the same cache key
    assert result == cache_key

    # Verify _store wasn't called - no new serialization
    client_data_store._store.assert_not_called()

    # Also test that deserializing works correctly
    deserialized = client_data_store.deserialize(cache_key)
    assert deserialized == large_data


def test_cache_key_in_complex_structure(mock_base_app: "MockPynenc") -> None:
    """Test that cache keys inside complex structures are preserved during serialization."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # First create some cache keys
    large_data1 = "x" * (client_data_store.conf.min_size_to_cache + 100)
    large_data2 = "y" * (client_data_store.conf.min_size_to_cache + 200)
    key1 = client_data_store.serialize(large_data1)
    key2 = client_data_store.serialize(large_data2)

    # Reset mocks
    client_data_store._store.reset_mock()

    # Create a complex structure with cache keys
    complex_data = {
        "normal_key": "small_value",
        "cached_key1": key1,
        "nested": {"cached_key2": key2, "list_with_keys": [1, 2, key1, key2]},
    }

    # Serialize the complex structure
    complex_key = client_data_store.serialize(complex_data)

    # Deserialize and check structure
    result = client_data_store.deserialize(complex_key)

    # Verify the cache keys in the complex structure were preserved
    assert result["cached_key1"] == key1
    assert result["nested"]["cached_key2"] == key2
    assert result["nested"]["list_with_keys"][2] == key1
    assert result["nested"]["list_with_keys"][3] == key2

    # Deserialize the keys in the complex structure
    assert client_data_store.deserialize(result["cached_key1"]) == large_data1
    assert client_data_store.deserialize(result["nested"]["cached_key2"]) == large_data2


def test_max_size_to_cache_enforcement(mock_base_app: "MockPynenc") -> None:
    """Test that max_size_to_cache prevents caching when exceeded."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Set a max size limit
    client_data_store.conf.max_size_to_cache = 5000

    # Value within limits should be cached
    medium_value = "x" * (client_data_store.conf.min_size_to_cache + 100)
    medium_result = client_data_store.serialize(medium_value)
    assert client_data_store.is_reference(medium_result)
    assert client_data_store._store.call_count == 1

    # Reset mock
    client_data_store._store.reset_mock()
    # Value above max should not be cached
    large_value = "x" * 10000
    large_result = client_data_store.serialize(large_value)
    assert not client_data_store.is_reference(large_result)
    assert large_result == mock_base_app.serializer.serialize(large_value)
    # _store should not be called
    client_data_store._store.assert_not_called()


def test_max_size_disabled_allows_large_values(mock_base_app: "MockPynenc") -> None:
    """Test that max_size_to_cache=0 disables the size limit."""
    client_data_store = MockClientDataStore(app=mock_base_app)

    # Ensure max_size is disabled (0)
    client_data_store.conf.max_size_to_cache = 0

    # Very large value should still attempt to be cached when limit is disabled
    very_large_value = "x" * 1_000_000
    result = client_data_store.serialize(very_large_value)
    # Should be cached (returns cache key)
    assert client_data_store.is_reference(result)
    # _store should be called since there's no size limit
    assert client_data_store._store.call_count == 1
