from unittest.mock import Mock

import pytest

from pynenc.arguments import Arguments
from pynenc.conf.config_pynenc import ArgumentPrintMode, ConfigPynenc


@pytest.fixture
def mock_app() -> Mock:
    """Create a mock app with config."""
    app = Mock()
    app.conf = ConfigPynenc()
    return app


def test_arguments_str_repr_no_app() -> None:
    """Test string representation without app instance."""
    long_string = "long_string" * 10
    args = Arguments(kwargs={"a": 1, "b": long_string})
    assert str(args) == "{a:1, b:long_stringlong_stringlong_strin...}"
    assert repr(args) in (
        f"Arguments({{'a', 'b'}}, id={args.args_id})",
        f"Arguments({{'b', 'a'}}, id={args.args_id})",
    )


def test_arguments_str_hidden(mock_app: Mock) -> None:
    """Test hidden arguments mode."""
    args = Arguments(kwargs={"a": 1, "b": 2}, app=mock_app)

    # Test HIDDEN mode
    mock_app.conf.argument_print_mode = ArgumentPrintMode.HIDDEN
    assert str(args) == "<arguments hidden>"

    # Test print_arguments = False
    mock_app.conf.argument_print_mode = ArgumentPrintMode.FULL
    mock_app.conf.print_arguments = False
    assert str(args) == "<arguments hidden>"


def test_arguments_str_keys(mock_app: Mock) -> None:
    """Test keys-only mode."""
    args = Arguments(kwargs={"a": 1, "b": "long_string" * 10}, app=mock_app)
    mock_app.conf.argument_print_mode = ArgumentPrintMode.KEYS
    assert str(args) == "{a, b}"


def test_arguments_str_truncated(mock_app: Mock) -> None:
    """Test truncated mode with different lengths."""
    long_string = "long_string" * 10
    args = Arguments(kwargs={"a": 1, "b": long_string}, app=mock_app)
    mock_app.conf.argument_print_mode = ArgumentPrintMode.TRUNCATED

    # Test with default truncation
    mock_app.conf.truncate_arguments_length = 32
    assert str(args) == f"{{a:1, b:{long_string[:32]}...}}"

    # Test with no truncation
    mock_app.conf.truncate_arguments_length = 0
    assert str(args) == f"{{a:1, b:{long_string}}}"

    # Test with short truncation
    mock_app.conf.truncate_arguments_length = 10
    assert str(args) == f"{{a:1, b:{long_string[:10]}...}}"


def test_arguments_str_full(mock_app: Mock) -> None:
    """Test full arguments mode."""
    long_string = "long_string" * 10
    args = Arguments(kwargs={"a": 1, "b": long_string}, app=mock_app)
    mock_app.conf.argument_print_mode = ArgumentPrintMode.FULL
    assert str(args) == f"{{a:1, b:{long_string}}}"


def test_arguments_str_empty(mock_app: Mock) -> None:
    """Test empty arguments representation."""
    args = Arguments(kwargs={}, app=mock_app)
    assert str(args) == "<no_args>"
