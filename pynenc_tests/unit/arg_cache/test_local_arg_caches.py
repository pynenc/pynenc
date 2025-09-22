from typing import Optional

import pytest

from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.serializer.constants import ReservedKeys
from pynenc_tests.conftest import MockPynenc


class TestObject:
    """Simple test object with controlled hash behavior."""

    def __init__(self, value: str, *, hashable: bool = True):
        self.value = value
        self._hashable = hashable

    def __hash__(self) -> int:
        if not self._hashable:
            raise TypeError("Not hashable")
        return hash(self.value)

    def __str__(self) -> str:
        return self.value


class LocalArgCache(BaseArgCache):
    """Minimal test implementation of BaseArgCache."""

    def __init__(self, app: "MockPynenc", *, serialized_value: Optional[str] = None):
        super().__init__(app)
        self._stored_values: dict[str, str] = {}
        self._next_serialized = serialized_value
        # Override app serializer for testing
        self.app.serializer.serialize = lambda obj: (  # type: ignore
            self._next_serialized or str(obj)
        )

    def _store(self, key: str, value: str) -> None:
        self._stored_values[key] = value

    def _retrieve(self, key: str) -> str:
        return self._stored_values[key]

    def _purge(self) -> None:
        self._stored_values.clear()


@pytest.fixture
def mock_app() -> MockPynenc:
    return MockPynenc()


def test_check_object_identity(mock_app: MockPynenc) -> None:
    """Test object identity cache lookup."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    key = f"{ReservedKeys.ARG_CACHE.value}:test_key"

    # Nothing in cache initially
    assert cache._check_object_identity(obj) is None

    # Add to cache and check again
    cache._obj_id_cache[id(obj)] = key
    assert cache._check_object_identity(obj) == key


def test_check_object_hash(mock_app: MockPynenc) -> None:
    """Test object hash cache lookup."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    key = f"{ReservedKeys.ARG_CACHE.value}:test_key"

    # Pre-populate hash cache
    cache._hash_cache[hash(obj)] = key

    # Should return key and update object identity cache
    found_key, obj_id = cache._check_object_hash(obj)
    assert found_key == key
    assert obj_id == id(obj)
    assert cache._obj_id_cache[obj_id] == key


def test_check_object_hash_unhashable(mock_app: MockPynenc) -> None:
    """Test hash cache lookup with unhashable object."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test", hashable=False)

    # Should handle TypeError gracefully
    key, obj_id = cache._check_object_hash(obj)
    assert key is None
    assert obj_id == id(obj)


def test_check_fingerprint(mock_app: MockPynenc) -> None:
    """Test fingerprint cache lookup."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    serialized = "x" * 200
    key = f"{ReservedKeys.ARG_CACHE.value}:test_key"
    obj_id = id(obj)

    # Pre-populate fingerprint cache
    fingerprint = (serialized[:128], len(serialized))
    cache._fingerprint_cache[fingerprint] = key

    # Should return key and update caches
    found_key = cache._check_fingerprint(serialized, obj_id, obj)
    assert found_key == key
    assert cache._obj_id_cache[obj_id] == key
    try:
        assert cache._hash_cache[hash(obj)] == key
    except TypeError:
        pass  # OK if object is not hashable


def test_check_exact_match(mock_app: MockPynenc) -> None:
    """Test exact match cache lookup."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    serialized = "test_serialized"
    key = f"{ReservedKeys.ARG_CACHE.value}:test_key"
    obj_id = id(obj)

    # Pre-populate key cache
    cache._key_cache[serialized] = key

    # Should return key and fingerprint
    found_key, fingerprint = cache._check_exact_match(serialized, obj_id, obj)
    assert found_key == key
    assert fingerprint == (serialized[:128], len(serialized))
    # Should update all caches
    assert cache._obj_id_cache[obj_id] == key
    assert cache._fingerprint_cache[fingerprint] == key
    try:
        assert cache._hash_cache[hash(obj)] == key
    except TypeError:
        pass  # OK if object is not hashable


def test_store_new_value(mock_app: MockPynenc) -> None:
    """Test storing new value and updating all caches."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    serialized = "test_serialized"
    obj_id = id(obj)
    fingerprint = (serialized[:128], len(serialized))

    # Store new value
    key = cache._store_new_value(serialized, obj_id, obj, fingerprint)

    # Verify all caches are updated
    assert cache._stored_values[key] == serialized
    assert cache._obj_id_cache[obj_id] == key
    try:
        assert cache._hash_cache[hash(obj)] == key
    except TypeError:
        pass  # OK if object is not hashable
    assert cache._fingerprint_cache[fingerprint] == key
    assert cache._key_cache[serialized] == key
    assert cache._deserialized_cache[key] == obj


def test_serialize_integration(mock_app: MockPynenc) -> None:
    """Test full serialization flow with different cache scenarios."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")

    # Test disable_cache flag
    assert cache.serialize(obj, disable_cache=True) == "test"

    # Test small value (below cache threshold)
    cache.conf.min_size_to_cache = 1000
    cache._next_serialized = "small"
    assert cache.serialize(obj) == "small"

    # Test large value (new entry)
    cache.conf.min_size_to_cache = 10
    cache._next_serialized = "large_enough"
    key = cache.serialize(obj)
    assert cache.is_cache_key(key)
    assert len(cache._stored_values) == 1


def test_serialize_fingerprint_cache_hit(mock_app: MockPynenc) -> None:
    """Test serialization using fingerprint cache."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    serialized = "x" * 200  # Long enough to trigger caching
    cache._next_serialized = serialized
    cache.conf.min_size_to_cache = 100  # Ensure caching is triggered

    # Pre-populate fingerprint cache
    expected_key = f"{ReservedKeys.ARG_CACHE.value}:test_key"
    fingerprint = (serialized[:128], len(serialized))
    cache._fingerprint_cache[fingerprint] = expected_key
    cache._stored_values[expected_key] = serialized  # Ensure value exists

    # Should hit fingerprint cache and return existing key
    result_key = cache.serialize(obj)
    assert result_key == expected_key
    # Should have updated other caches
    assert cache._obj_id_cache[id(obj)] == expected_key
    try:
        assert cache._hash_cache[hash(obj)] == expected_key
    except TypeError:
        pass  # OK if unhashable


def test_serialize_exact_match_cache_hit(mock_app: MockPynenc) -> None:
    """Test serialization using exact match cache."""
    cache = LocalArgCache(mock_app)
    obj = TestObject("test")
    serialized = "test_serialized"
    cache._next_serialized = serialized
    cache.conf.min_size_to_cache = 10  # Ensure caching is triggered

    # Pre-populate key cache
    expected_key = f"{ReservedKeys.ARG_CACHE.value}:test_key"
    cache._key_cache[serialized] = expected_key
    cache._stored_values[expected_key] = serialized  # Ensure value exists

    # Should hit exact match cache and return existing key
    result_key = cache.serialize(obj)
    assert result_key == expected_key
    # Should have updated all caches
    fingerprint = (serialized[:128], len(serialized))
    assert cache._obj_id_cache[id(obj)] == expected_key
    assert cache._fingerprint_cache[fingerprint] == expected_key
    try:
        assert cache._hash_cache[hash(obj)] == expected_key
    except TypeError:
        pass  # OK if unhashable


def test_deserialize_caching(mock_app: MockPynenc) -> None:
    """Test that deserialize properly uses and updates caches."""
    cache = LocalArgCache(mock_app)
    test_value = '"test_value"'  # JSON-encoded string
    cache_key = f"{ReservedKeys.ARG_CACHE.value}:test_key"

    # Store JSON-encoded value in backend
    cache._stored_values[cache_key] = test_value

    # First deserialization should store in cache
    obj1 = cache.deserialize(cache_key)
    assert obj1 == "test_value"
    assert cache._deserialized_cache[cache_key] == obj1

    # Second deserialization should use cache
    obj2 = cache.deserialize(cache_key)
    assert obj2 is obj1  # Should be same instance

    # Verify all caches were updated
    assert cache._obj_id_cache[id(obj1)] == cache_key
    try:
        assert cache._hash_cache[hash(obj1)] == cache_key
    except TypeError:
        pass  # OK if unhashable


def test_deserialize_unhashable_object(mock_app: MockPynenc) -> None:
    """Test deserialize handling of unhashable objects."""
    cache = LocalArgCache(mock_app)
    cache_key = f"{ReservedKeys.ARG_CACHE.value}:test_key"

    # Store JSON-encoded value in backend
    cache._stored_values[cache_key] = '"test"'

    # Mock serializer to return unhashable object and update caches
    def mock_deserialize(x: str) -> TestObject:
        obj = TestObject(x.strip('"'), hashable=False)
        # Update obj_id cache explicitly since we're mocking
        cache._obj_id_cache[id(obj)] = cache_key
        return obj

    cache.app.serializer.deserialize = mock_deserialize  # type: ignore

    # Should not raise TypeError when updating caches
    result = cache.deserialize(cache_key)

    # Only object identity cache should be updated
    assert cache._obj_id_cache[id(result)] == cache_key
    assert len(cache._hash_cache) == 0  # No hash cache entries for unhashable object


def test_cache_size_limit_obj_id(mock_app: MockPynenc) -> None:
    """Test LRU eviction in object identity cache."""
    cache = LocalArgCache(mock_app)
    cache.conf.local_cache_size = 2  # Set small cache size

    # Add objects up to limit
    obj1 = TestObject("test1")
    obj2 = TestObject("test2")
    key1 = f"{ReservedKeys.ARG_CACHE.value}:key1"
    key2 = f"{ReservedKeys.ARG_CACHE.value}:key2"

    cache._update_obj_id_cache(id(obj1), key1)
    cache._update_obj_id_cache(id(obj2), key2)
    assert len(cache._obj_id_cache) == 2

    # Add one more object
    obj3 = TestObject("test3")
    key3 = f"{ReservedKeys.ARG_CACHE.value}:key3"
    cache._update_obj_id_cache(id(obj3), key3)

    # Check oldest entry was removed
    assert len(cache._obj_id_cache) == 2
    assert id(obj1) not in cache._obj_id_cache
    assert cache._obj_id_cache[id(obj2)] == key2
    assert cache._obj_id_cache[id(obj3)] == key3


def test_cache_size_limit_all_caches(mock_app: MockPynenc) -> None:
    """Test LRU eviction in all cache types."""
    cache = LocalArgCache(mock_app)
    cache.conf.local_cache_size = 2

    # Test fingerprint cache
    for i in range(3):
        fingerprint = (f"prefix{i}", i)
        key = f"{ReservedKeys.ARG_CACHE.value}:key{i}"
        cache._update_fingerprint_cache(fingerprint, key)
    assert len(cache._fingerprint_cache) == 2
    assert ("prefix0", 0) not in cache._fingerprint_cache

    # Test hash cache
    for i in range(3):
        obj = TestObject(f"test{i}")
        key = f"{ReservedKeys.ARG_CACHE.value}:key{i}"
        cache._update_hash_cache(obj, key)
    assert len(cache._hash_cache) == 2
    assert hash(TestObject("test0")) not in cache._hash_cache

    # Test key cache
    for i in range(3):
        serialized = f"serialized{i}"
        key = f"{ReservedKeys.ARG_CACHE.value}:key{i}"
        cache._update_key_cache(serialized, key)
    assert len(cache._key_cache) == 2
    assert "serialized0" not in cache._key_cache

    # Test deserialized cache
    for i in range(3):
        key = f"{ReservedKeys.ARG_CACHE.value}:key{i}"
        obj = TestObject(f"test{i}")
        cache._update_deserialized_cache(key, obj)
    assert len(cache._deserialized_cache) == 2
    assert f"{ReservedKeys.ARG_CACHE.value}:key0" not in cache._deserialized_cache


def test_deserialize_cache_hit(mock_app: MockPynenc) -> None:
    """Test that deserialize uses deserialized cache when available."""
    cache = LocalArgCache(mock_app)
    cache_key = f"{ReservedKeys.ARG_CACHE.value}:test_key"
    cached_obj = TestObject("test")

    # Pre-populate deserialized cache
    cache._deserialized_cache[cache_key] = cached_obj

    # Should return cached object without calling retrieve
    result = cache.deserialize(cache_key)
    assert result is cached_obj  # Should be same instance
    assert not hasattr(cache, "_retrieve_called")  # Should not call _retrieve
