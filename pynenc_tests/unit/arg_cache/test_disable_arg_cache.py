import pytest

from pynenc.arg_cache.disabled_arg_cache import DisabledArgCache
from pynenc_tests.conftest import MockPynenc


@pytest.fixture
def mock_app() -> MockPynenc:
    return MockPynenc()


@pytest.fixture
def disabled_cache(mock_app: MockPynenc) -> DisabledArgCache:
    return DisabledArgCache(mock_app)


def test_store_raises_not_implemented(disabled_cache: DisabledArgCache) -> None:
    """Test that _store raises NotImplementedError."""
    with pytest.raises(
        NotImplementedError, match="DisabledArgCache does not support storing values"
    ):
        disabled_cache._store("test_key", "test_value")


def test_retrieve_raises_not_implemented(disabled_cache: DisabledArgCache) -> None:
    """Test that _retrieve raises NotImplementedError."""
    with pytest.raises(
        NotImplementedError, match="DisabledArgCache does not support retrieving values"
    ):
        disabled_cache._retrieve("test_key")


def test_serialize_passthrough(disabled_cache: DisabledArgCache) -> None:
    """Test that serialize passes directly to serializer."""
    test_value = "test_value"
    result = disabled_cache.serialize(test_value)
    # Should match what the app serializer would do directly
    assert result == disabled_cache.app.serializer.serialize(test_value)


def test_deserialize_passthrough(disabled_cache: DisabledArgCache) -> None:
    """Test that deserialize passes directly to serializer."""
    test_value = "test_value"
    serialized = disabled_cache.app.serializer.serialize(test_value)
    result = disabled_cache.deserialize(serialized)
    assert result == test_value


def test_purge_no_op(disabled_cache: DisabledArgCache) -> None:
    """Test that purge is a no-op."""
    # Should not raise any exception
    disabled_cache.purge()


def test_disable_cache_flag_ignored(disabled_cache: DisabledArgCache) -> None:
    """Test that disable_cache flag has no effect."""
    test_value = "test_value"
    result1 = disabled_cache.serialize(test_value)
    result2 = disabled_cache.serialize(test_value, disable_cache=True)
    assert result1 == result2
