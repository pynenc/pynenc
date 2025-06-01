from unittest.mock import Mock, call

import pytest

from pynenc.util.redis_keys import Key, sanitize_for_redis


# Test for sanitize_for_redis function
def test_sanitize_for_redis() -> None:
    test_str = "[Hello*World]"
    sanitized = sanitize_for_redis(test_str)
    assert sanitized == "__OPEN_BRACKET__Hello__ASTERISK__World__CLOSE_BRACKET__"


@pytest.fixture
def key_instance() -> Key:
    return Key("test_app", "test_prefix")


def test_key_initialization(key_instance: Key) -> None:
    assert key_instance.prefix.startswith("__pynenc__:")


def test_key_invocation(key_instance: Key) -> None:
    assert (
        key_instance.invocation("123")
        == "__pynenc__:test_app:test_prefix:invocation:123"
    )


def test_key_task(key_instance: Key) -> None:
    assert (
        key_instance.task("task123") == "__pynenc__:test_app:test_prefix:task:task123"
    )


def test_key_purge() -> None:
    # Setup mock Redis client
    mock_redis = Mock()

    # Set up the scan_iter to return multiple batches of keys
    mock_redis.scan_iter.return_value = [
        f"key{i}"
        for i in range(1, 2501)  # Create a large list of keys to test batching
    ]

    # Create key instance and run purge
    key = Key("app", "prefix")
    key.purge(mock_redis)

    # Verify scan_iter was called with correct parameters
    mock_redis.scan_iter.assert_called_once_with(f"{key.prefix}*", count=1000)

    # Verify that delete was called with batches of 1000 keys
    expected_calls = [
        call(*[f"key{i}" for i in range(1, 1001)]),  # First batch
        call(*[f"key{i}" for i in range(1001, 2001)]),  # Second batch
        call(*[f"key{i}" for i in range(2001, 2501)]),  # Last batch (partial)
    ]

    # Check if delete was called with the expected batches
    assert mock_redis.delete.call_count == 3
    mock_redis.delete.assert_has_calls(expected_calls, any_order=False)


def test_key_purge_empty() -> None:
    """Test purge when no keys are found."""
    mock_redis = Mock()
    mock_redis.scan_iter.return_value = []

    key = Key("app", "prefix")
    key.purge(mock_redis)

    # Verify scan_iter was called but delete was not
    mock_redis.scan_iter.assert_called_once_with(f"{key.prefix}*", count=1000)
    mock_redis.delete.assert_not_called()


def test_key_purge_small_batch() -> None:
    """Test purge with a small batch that doesn't need splitting."""
    mock_redis = Mock()
    mock_redis.scan_iter.return_value = [f"key{i}" for i in range(1, 11)]

    key = Key("app", "prefix")
    key.purge(mock_redis)

    # Verify scan_iter was called
    mock_redis.scan_iter.assert_called_once_with(f"{key.prefix}*", count=1000)

    # Verify delete was called once with all keys
    mock_redis.delete.assert_called_once_with(*[f"key{i}" for i in range(1, 11)])


def test_key_with_empty_prefix() -> None:
    with pytest.raises(ValueError) as exc_info:
        Key("app", "")
    assert "Prefix cannot be an empty string or None" in str(exc_info.value)


def test_key_with_none_prefix() -> None:
    with pytest.raises(ValueError) as exc_info:
        Key("app", None)  # type: ignore
    assert "Prefix cannot be an empty string or None" in str(exc_info.value)


# Test for invalid prefix
def test_key_invalid_prefix() -> None:
    with pytest.raises(ValueError):
        Key("test_app", "")


# Test for different app_id and prefix sanitization
def test_key_sanitization() -> None:
    key = Key("app[id]", "pre*fix")
    assert (
        key.prefix
        == "__pynenc__:app__OPEN_BRACKET__id__CLOSE_BRACKET__:pre__ASTERISK__fix:"
    )


def test_app_id_with_colon() -> None:
    with pytest.raises(ValueError) as exc_info:
        Key("app:id", "prefix")
    assert "App ID cannot contain ':'" in str(exc_info.value)
