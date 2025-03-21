from pynenc.builder import PynencBuilder
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.conf.config_task import ConcurrencyControlType


def test_valid_concurrency_modes_matches_enum() -> None:
    """
    Test that _VALID_CONCURRENCY_MODES in PynencBuilder matches the values in ConcurrencyControlType.
    """
    # Get the enum values from ConcurrencyControlType
    enum_values = set(ConcurrencyControlType.__members__.keys())

    # Get the valid modes from PynencBuilder
    builder_modes = PynencBuilder._VALID_CONCURRENCY_MODES

    # Assert they match exactly
    assert (
        builder_modes == enum_values
    ), f"_VALID_CONCURRENCY_MODES {builder_modes} does not match ConcurrencyControlType values {enum_values}"


def test_argument_print_mode_docstring_contains_all_values() -> None:
    """
    Test that the docstring of argument_print_mode contains all values from ArgumentPrintMode.
    """
    # Get the enum values from ArgumentPrintMode
    expected_values = set(ArgumentPrintMode.__members__.keys())

    # Get the docstring from the argument_print_mode method
    docstring = PynencBuilder.argument_print_mode.__doc__
    assert docstring is not None, "argument_print_mode method has no docstring"

    # Check that each enum value is present in the docstring
    for value in expected_values:
        assert (
            value in docstring
        ), f"Enum value '{value}' from ArgumentPrintMode not found in argument_print_mode docstring"
