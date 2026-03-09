from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pynenc import Pynenc


@pytest.mark.parametrize("cache_attr", ["client_data_store"])
def test_mem_client_data_store_store_retrieve_purge(
    app_instance: "Pynenc", cache_attr: str
) -> None:
    """
    Test store, retrieve, and purge methods for client_data_store implementations.

    :param app_instance: Fixture providing a Pynenc app instance
    :param cache_attr: Attribute name for the cache instance
    """
    cache = getattr(app_instance, cache_attr)
    key = "test-key"
    value = "test-value"

    # Store value
    cache._store(key, value)
    assert cache._retrieve(key) == value

    # Purge cache
    cache._purge()
    with pytest.raises(KeyError):
        cache._retrieve(key)


@pytest.mark.parametrize("cache_attr", ["client_data_store"])
def test_mem_client_data_store_serialize_should_bypass_cache_when_disabled(
    app_instance: "Pynenc", cache_attr: str
) -> None:
    """
    Verify disabling client data store bypasses reference-key caching.

    :param Pynenc app_instance: Fixture providing a Pynenc app instance
    :param str cache_attr: Attribute name for the cache instance
    """
    cache = getattr(app_instance, cache_attr)
    cache.conf.disable_client_data_store = True
    large_value = "x" * 1_000_000

    serialized = cache.serialize(large_value)

    assert serialized == app_instance.serializer.serialize(large_value)
    assert not cache.is_reference(serialized)
