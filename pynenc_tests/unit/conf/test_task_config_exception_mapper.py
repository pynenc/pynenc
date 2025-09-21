import pytest

from pynenc.conf.config_task import exception_config_mapper, exception_mapper


def test_exception_mapper_with_string_representation() -> None:
    """Test mapping from string representation to exception class."""
    input_value = ["builtins.ValueError"]
    expected_output = (ValueError,)
    assert exception_mapper(input_value) == expected_output


def test_exception_mapper_with_exception_class() -> None:
    """Test handling direct exception class input."""
    input_value: list[type[Exception]] = [ValueError]
    expected_output = (ValueError,)
    assert exception_mapper(input_value) == expected_output


def test_exception_mapper_invalid_string_format() -> None:
    """Test error handling for invalid string format."""
    input_value = ["nonexistent_module.NonexistentException"]
    with pytest.raises(TypeError):
        exception_mapper(input_value)


def test_exception_mapper_non_exception_type() -> None:
    """Test error handling for non-exception types."""
    input_value = ["builtins.int"]
    with pytest.raises(TypeError):
        exception_mapper(input_value)


def test_exception_mapper_invalid_types() -> None:
    """Test error handling for invalid types (neither string nor exception class)."""
    input_value = [123, "builtins.ValueError"]
    with pytest.raises(TypeError):
        exception_mapper(input_value)  # type: ignore


# def test_exception_mapper_with_invalid_type_input() -> None:
#     """Test error handling for input that is neither a string nor an Exception subclass."""
#     input_value = [ValueError, 123]  # 123 is neither a string nor an Exception subclass
#     with pytest.raises(TypeError) as exc_info:
#         exception_mapper(input_value)  # type: ignore
#     assert "Expected str or Exception subclass" in str(exc_info.value)


def test_exception_config_mapper_valid_input() -> None:
    """Test with valid input strings."""
    input_value = ["builtins.ValueError", "builtins.KeyError"]
    expected_output = (ValueError, KeyError)
    assert exception_config_mapper(input_value, tuple) == expected_output


def test_exception_config_mapper_invalid_input_string() -> None:
    """Test with an invalid input string."""
    input_value = ["nonexistent_module.NonexistentException"]
    with pytest.raises(TypeError):
        exception_config_mapper(input_value, tuple)


def test_exception_config_mapper_incorrect_expected_type() -> None:
    """Test with incorrect expected type."""
    input_value = ["builtins.ValueError", "builtins.KeyError"]
    with pytest.raises(TypeError):
        # Here we pass a type that we know the output won't match, like list
        exception_config_mapper(input_value, list)
