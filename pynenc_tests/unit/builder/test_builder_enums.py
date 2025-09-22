import pytest

from pynenc.builder import PynencBuilder
from pynenc.conf.config_pynenc import ArgumentPrintMode


def test_argument_print_mode_should_accept_all_enum_values() -> None:
    """
    Test that argument_print_mode accepts all ArgumentPrintMode enum values.

    This is more important than docstring content - the method should actually work
    with all valid enum values.
    """
    # Test that each enum value can be used successfully
    for mode_name, mode_value in ArgumentPrintMode.__members__.items():
        # Test with enum value
        builder1 = PynencBuilder().argument_print_mode(mode=mode_value)
        app1 = builder1.build()
        assert app1.conf.argument_print_mode == mode_value

        # Test with string value
        builder2 = PynencBuilder().argument_print_mode(mode=mode_name)
        app2 = builder2.build()
        assert app2.conf.argument_print_mode == mode_value


def test_argument_print_mode_should_have_comprehensive_docstring() -> None:
    """
    Test that argument_print_mode has a meaningful docstring.

    Focus on essential documentation rather than exact enum value listing.
    """
    docstring = PynencBuilder.argument_print_mode.__doc__
    assert docstring is not None, "argument_print_mode method should have a docstring"

    # Check for essential documentation elements
    assert (
        "Configure how task arguments are printed" in docstring
        or "argument" in docstring.lower()
    )
    assert "mode" in docstring.lower()

    # Should mention the parameter and return type
    assert ":param" in docstring
    assert ":return" in docstring


def test_argument_print_mode_should_reject_invalid_values() -> None:
    """
    Test that argument_print_mode properly validates input values.
    """
    builder = PynencBuilder()

    # Should work with valid values
    valid_modes = list(ArgumentPrintMode.__members__.keys())
    for mode in valid_modes:
        # This should not raise an exception
        builder.argument_print_mode(mode=mode)

    # Should reject invalid string values when built
    # (Note: We test this at build time since that's when validation typically occurs)
    try:
        PynencBuilder().argument_print_mode(mode="INVALID_MODE").build()
        raise AssertionError("Should have raised an exception for invalid mode")
    except (ValueError, AttributeError, KeyError):
        # Any of these exceptions are acceptable for invalid input
        pass


def test_invalid_config_enum() -> None:
    """Test that invalid enum values raise appropriate errors."""
    with pytest.raises(ValueError):
        PynencBuilder().argument_print_mode(mode="INVALID_MODE").build()
        raise AssertionError("Expected ValueError for invalid enum value")
