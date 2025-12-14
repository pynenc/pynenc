"""
Unit tests for pynenc/util/import_tools.py module.

Tests the is_module_level_function utility which validates that functions
can be properly serialized and imported.
"""

from typing import Any

from pynenc.util.import_tools import is_module_level_function


def test_is_module_level_function_returns_true_for_module_functions() -> None:
    """Test that module-level functions return True."""
    result = is_module_level_function(is_module_level_function)
    assert result is True


def test_is_module_level_function_returns_false_for_lambda() -> None:
    """Test that lambda functions return False."""
    lambda_func = lambda x: x * 2  # noqa: E731
    result = is_module_level_function(lambda_func)
    assert result is False


def test_is_module_level_function_returns_false_for_nested_function() -> None:
    """Test that nested functions return False."""

    def outer() -> Any:
        def inner() -> None:
            pass

        return inner

    nested_func = outer()
    result = is_module_level_function(nested_func)
    assert result is False


def test_is_module_level_function_returns_false_for_class_method() -> None:
    """Test that class methods return False."""

    class MyClass:
        def my_method(self) -> None:
            pass

    result = is_module_level_function(MyClass.my_method)
    assert result is False


def test_is_module_level_function_returns_false_for_non_callable() -> None:
    """Test that non-callable objects return False."""
    result = is_module_level_function("not a function")  # type: ignore[arg-type]
    assert result is False


def test_is_module_level_function_returns_false_for_main_module() -> None:
    """Test that functions from __main__ module return False."""

    def main_func() -> None:
        pass

    # Manually set __module__ to __main__
    main_func.__module__ = "__main__"
    result = is_module_level_function(main_func)
    assert result is False


def test_is_module_level_function_returns_false_when_qualname_differs() -> None:
    """Test that functions with non-matching __qualname__ return False."""

    class Container:
        pass

    def func_outside_class() -> None:
        pass

    # Manually set __qualname__ to include class name
    func_outside_class.__qualname__ = f"{Container.__name__}.method"
    result = is_module_level_function(func_outside_class)
    assert result is False
