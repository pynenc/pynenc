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
    mock_redis = Mock()
    mock_redis.scan_iter.return_value = ["key1", "key2", "key3"]
    key = Key("app", "prefix")
    key.purge(mock_redis)
    mock_redis.delete.assert_has_calls(
        [call("key1"), call("key2"), call("key3")], any_order=True
    )


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
