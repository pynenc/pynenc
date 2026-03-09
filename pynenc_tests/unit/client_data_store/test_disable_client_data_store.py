import pytest

from pynenc.client_data_store.mem_client_data_store import MemClientDataStore

from pynenc_tests.conftest import MockPynenc


@pytest.fixture
def mock_app() -> MockPynenc:
    return MockPynenc()


@pytest.fixture
def disabled_store(mock_app: MockPynenc) -> MemClientDataStore:
    store = MemClientDataStore(mock_app)
    store.conf.disable_client_data_store = True
    return store


def test_serialize_passthrough(disabled_store: MemClientDataStore) -> None:
    """Test that serialize passes directly to serializer."""
    test_value = "test_value"
    result = disabled_store.serialize(test_value)
    # Should match what the app serializer would do directly
    assert result == disabled_store.app.serializer.serialize(test_value)


def test_deserialize_passthrough(disabled_store: MemClientDataStore) -> None:
    """Test that deserialize passes directly to serializer."""
    test_value = "test_value"
    serialized = disabled_store.app.serializer.serialize(test_value)
    result = disabled_store.deserialize(serialized)
    assert result == test_value


def test_purge_no_op(disabled_store: MemClientDataStore) -> None:
    """Test that purge is a no-op."""
    # Should not raise any exception
    disabled_store.purge()


def test_disable_cache_flag_ignored(disabled_store: MemClientDataStore) -> None:
    """Test that disable_cache flag has no effect."""
    test_value = "test_value"
    result1 = disabled_store.serialize(test_value)
    result2 = disabled_store.serialize(test_value, disable_cache=True)
    assert result1 == result2
