import importlib
import json
from typing import Any, Callable

from pynenc.util.import_tools import is_module_level_function


class NonModuleLevelFunctionError(ValueError):
    """
    Raised when a function provided for argument filtering is not defined at module level.

    Module-level functions are required for proper serialization and deserialization
    of argument filters.
    """

    pass


class SerializableCallable:
    """
    Wrapper for callable objects that enables serialization and deserialization.

    This class stores the module and name of a function to allow importing it later.
    """

    def __init__(self, func: Callable) -> None:
        """
        Initialize with a function reference.

        :param func: The callable to wrap
        :raises NonModuleLevelFunctionError: If the function is not at module level
        """
        if not is_module_level_function(func):
            raise NonModuleLevelFunctionError(
                f"Function {func} is not defined at module level. "
                f"Only module-level named functions can be used for argument filters."
            )
        self._func = func

    def callable_id(self) -> str:
        """
        Get a unique identifier for the callable.

        :return: A string representing the callable's module and name
        """
        return f"{self._func.__module__}.{self._func.__name__}"

    def __call__(self, *args: Any, **kwargs: Any) -> Any:
        """
        Call the wrapped function.

        :param args: Positional arguments to pass to the function
        :param kwargs: Keyword arguments to pass to the function
        :return: The result of the function call
        """
        return self._func(*args, **kwargs)

    def to_json(self) -> str:
        """
        Serialize this callable to a JSON string.

        :return: JSON string representation of this callable
        """
        return json.dumps(
            {"module": self._func.__module__, "function": self._func.__name__}
        )

    @classmethod
    def from_json(cls, json_str: str) -> "SerializableCallable":
        """
        Create a SerializableCallable instance from a JSON string.

        :param json_str: JSON string containing serialized callable data
        :return: A new SerializableCallable instance
        """
        data = json.loads(json_str)
        module = importlib.import_module(data["module"])
        return cls(getattr(module, data["function"]))
