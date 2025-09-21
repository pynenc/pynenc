import pytest

from pynenc import Pynenc
from pynenc.builder import PynencBuilder
from pynenc.conf.config_pynenc import ArgumentPrintMode


def test_hide_arguments_should_configure_correctly() -> None:
    """Test hide_arguments configuration."""
    app = PynencBuilder().hide_arguments().build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN
    # truncate_arguments_length is not set by this method, so it should be the default (32)
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_argument_keys_should_configure_correctly() -> None:
    """Test show_argument_keys configuration."""
    app = PynencBuilder().show_argument_keys().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.KEYS
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_full_arguments_should_configure_correctly() -> None:
    """Test show_full_arguments configuration."""
    app = PynencBuilder().show_full_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.FULL
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_truncated_arguments_should_use_default_length() -> None:
    """Test show_truncated_arguments with default truncate_length."""
    app = PynencBuilder().show_truncated_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 32  # Default value


def test_show_truncated_arguments_should_accept_custom_length() -> None:
    """Test show_truncated_arguments with a custom truncate_length."""
    app = PynencBuilder().show_truncated_arguments(truncate_length=100).build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 100  # Custom value


def test_argument_print_mode_should_accept_string_mode() -> None:
    """Test argument_print_mode with string input."""
    app = PynencBuilder().argument_print_mode(mode="HIDDEN").build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN


def test_argument_print_mode_should_match_default_truncate_length() -> None:
    """Test that the default truncate_length matches the config default."""
    app_builder = PynencBuilder().argument_print_mode(mode="HIDDEN").build()
    app = Pynenc()
    assert (
        app_builder.conf.truncate_arguments_length == app.conf.truncate_arguments_length
    )


def test_show_truncated_arguments_should_reject_invalid_length() -> None:
    """Test show_truncated_arguments with an invalid (non-positive) truncate_length."""
    builder = PynencBuilder()
    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=0).build()

    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=-1).build()
