from typing import TYPE_CHECKING

import pytest

if TYPE_CHECKING:
    from pynenc import Pynenc


@pytest.mark.parametrize("cache_attr", ["arg_cache"])
def test_mem_arg_cache_store_retrieve_purge(
    app_instance: "Pynenc", cache_attr: str
) -> None:
    """
    Test store, retrieve, and purge methods for arg_cache implementations.

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
