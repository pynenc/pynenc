from typing import TYPE_CHECKING

import pytest

from pynenc.arg_cache.base_arg_cache import BaseArgCache
from pynenc.serializer.constants import ReservedKeys
from tests import util
from tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    """Generate test matrix for all non-mock ArgCache implementations."""
    subclasses = [
        c
        for c in BaseArgCache.__subclasses__()
        if "mock" not in c.__name__.lower() and "disabled" not in c.__name__.lower()
    ]
    if "app" in metafunc.fixturenames:
        metafunc.parametrize("app", subclasses, indirect=True)


@pytest.fixture
def app(request: "FixtureRequest") -> MockPynenc:
    """Create a test app with the specified ArgCache implementation."""
    app = MockPynenc(app_id="int-arg_cache" + util.get_unique_id())
    app.arg_cache = request.param(app)
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_store_and_retrieve(app: MockPynenc) -> None:
    """Test basic storage and retrieval of values."""
    test_value = "x" * (app.arg_cache.conf.min_size_to_cache + 100)
    key = app.arg_cache.serialize(test_value)

    # Verify it's a cache key
    assert app.arg_cache.is_cache_key(key)
    assert key.startswith(f"{ReservedKeys.ARG_CACHE.value}:")

    # Verify retrieval
    retrieved = app.arg_cache.deserialize(key)
    assert retrieved == test_value


def test_cache_threshold(app: MockPynenc) -> None:
    """Test that only values above threshold are cached."""
    small_value = "small"
    large_value = "x" * (app.arg_cache.conf.min_size_to_cache + 100)

    # Small value shouldn't be cached
    small_result = app.arg_cache.serialize(small_value)
    assert not app.arg_cache.is_cache_key(small_result)
    assert small_result == app.serializer.serialize(small_value)

    # Large value should be cached
    large_result = app.arg_cache.serialize(large_value)
    assert app.arg_cache.is_cache_key(large_result)


def test_cache_reuse(app: MockPynenc) -> None:
    """Test that identical values reuse cache entries."""
    test_value = "x" * (app.arg_cache.conf.min_size_to_cache + 100)

    # First serialization
    key1 = app.arg_cache.serialize(test_value)

    # Second serialization of same value
    key2 = app.arg_cache.serialize(test_value)

    # Should get same cache key
    assert key1 == key2


def test_disabled_cache(app: MockPynenc) -> None:
    """Test that DisabledArgCache bypasses caching."""
    from pynenc.arg_cache.disabled_arg_cache import DisabledArgCache

    app.arg_cache = DisabledArgCache(app)  # type: ignore
    test_value = "x" * 1000000  # Large value

    # Should return serialized value directly
    result = app.arg_cache.serialize(test_value)
    assert result == app.serializer.serialize(test_value)
    assert not app.arg_cache.is_cache_key(result)


def test_cache_purge(app: MockPynenc) -> None:
    """Test cache purging functionality."""
    test_value = "x" * (app.arg_cache.conf.min_size_to_cache + 100)
    key = app.arg_cache.serialize(test_value)

    # Verify value is cached
    assert app.arg_cache.deserialize(key) == test_value

    # Purge cache
    app.arg_cache.purge()

    # Verify value is no longer cached
    with pytest.raises(KeyError):
        app.arg_cache.deserialize(key)


def test_disable_cache_flag(app: MockPynenc) -> None:
    """Test that disable_cache flag prevents caching."""
    test_value = "x" * (app.arg_cache.conf.min_size_to_cache + 100)

    # Normal serialization
    key1 = app.arg_cache.serialize(test_value)
    assert app.arg_cache.is_cache_key(key1)

    # Disabled cache serialization
    result = app.arg_cache.serialize(test_value, disable_cache=True)
    assert not app.arg_cache.is_cache_key(result)
    assert result == app.serializer.serialize(test_value)
