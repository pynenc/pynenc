"""
Argument filters for trigger-based task executions.

This module defines abstractions and implementations for filtering task arguments
based on various conditions. It enables flexible matching of task arguments
against filter criteria.
"""

import json
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, ClassVar, cast

from pynenc.arguments import Arguments
from pynenc.trigger.arguments.arguments_common import SerializableCallable
from pynenc.util.subclasses import build_class_cache

if TYPE_CHECKING:
    from pynenc.app import Pynenc


class ArgumentFilter(ABC):
    """
    Base class for argument filters.

    Argument filters determine if task arguments match certain filter criteria.
    """

    _argument_filter_class_cache: ClassVar[dict[str, type["ArgumentFilter"]]] = {}
    _cache_initialized: ClassVar[bool] = False

    @classmethod
    def _initialize_class_cache(cls) -> None:
        """
        Initialize the argument filter class cache by recursively finding all subclasses.
        """
        if cls._cache_initialized:
            return

        cls._argument_filter_class_cache = build_class_cache(cls)
        cls._cache_initialized = True

    @classmethod
    def get_argument_filter_class(
        cls, argument_filter_type: str
    ) -> type["ArgumentFilter"] | None:
        """
        Get an argument filter class by its name from the class cache.

        :param argument_filter_type: Name of the argument filter class to find
        :return: The argument filter class or None if not found
        """
        if not cls._cache_initialized:
            cls._initialize_class_cache()

        return cls._argument_filter_class_cache.get(argument_filter_type)

    @cached_property
    @abstractmethod
    def filter_id(self) -> str:
        """
        Unique identifier for this argument filter.

        This identifier is used to distinguish between different argument filter types.
        """
        pass

    @abstractmethod
    def filter_arguments(self, arguments: dict[str, Any]) -> bool:
        """
        Check if the given arguments match the filter criteria.

        :param arguments: Task arguments to check
        :return: True if arguments match the filter, False otherwise
        """
        pass

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this argument filter to a JSON string.

        :param app: Pynenc application instance for serializing complex arguments
        :return: JSON string representation of this argument filter
        """
        data = self._to_json(app)
        return json.dumps({"filter_type": self.__class__.__name__, "data": data})

    @abstractmethod
    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this argument filter.

        Subclasses must implement this method to handle their specific serialization logic.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized argument filter data
        """
        pass

    @classmethod
    def from_json(cls, json_str: str, app: "Pynenc") -> "ArgumentFilter":
        """
        Create an argument filter instance from a JSON string.

        This is a factory method that instantiates the correct subclass based on the
        filter_type field in the JSON data.

        :param json_str: JSON string containing serialized argument filter data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of the appropriate ArgumentFilter subclass
        :raises ValueError: If the JSON data is invalid or the filter type is unknown
        """
        try:
            data_dict = json.loads(json_str)
            filter_type = data_dict.get("filter_type")
            data = data_dict.get("data", {})

            # Find the appropriate argument filter class using the class cache
            filter_class = cls.get_argument_filter_class(filter_type)

            if filter_class is None:
                raise ValueError(f"Unknown argument filter type: {filter_type}")

            return filter_class._from_json(data, app)

        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON for argument filter") from e

    @classmethod
    @abstractmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ArgumentFilter":
        """
        Create an argument filter instance from parsed JSON data.

        Each subclass must implement this method to handle its specific deserialization logic.

        :param data: Dictionary with argument filter data from JSON
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of this argument filter class
        :raises ValueError: If the data is invalid for this argument filter type
        """
        pass


class StaticArgumentFilter(ArgumentFilter):
    """
    Filters task arguments based on exact matching with provided arguments.

    This is the traditional approach using the Arguments class.
    """

    def __init__(self, arguments: dict[str, Any]) -> None:
        """
        Initialize with filter arguments.

        :param arguments: Arguments that task arguments must match
        """
        self.arguments = Arguments(arguments)

    @cached_property
    def filter_id(self) -> str:
        """
        Generate a unique ID for this argument filter.

        The ID is based on the hash of the filter arguments.
        """
        return f"static_{self.arguments.args_id}"

    def filter_arguments(self, arguments: dict[str, Any]) -> bool:
        """
        Check if the given arguments exactly match the filter arguments.

        :param arguments: Task arguments to check
        :return: True if arguments match the filter, False otherwise
        """
        # Check if each filter argument matches the corresponding task argument
        for key, value in self.arguments.kwargs.items():
            if key not in arguments or arguments[key] != value:
                return False
        return True

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this argument filter.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized argument filter data
        """
        return {"arguments": self.arguments.to_json(app)}

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "StaticArgumentFilter":
        """
        Create an StaticArgumentFilter instance from parsed JSON data.

        :param data: Dictionary with argument filter data from JSON
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new StaticArgumentFilter instance
        """
        arguments = Arguments.from_json(app, data.get("arguments", {}))
        return cls(arguments.kwargs)

    def __eq__(self, other: Any) -> bool:
        """
        Check equality with another object.

        :param other: Object to compare with
        :return: True if equal, False otherwise
        """
        if isinstance(other, StaticArgumentFilter):
            return self.arguments == other.arguments
        return False


class CallableArgumentFilter(ArgumentFilter):
    """
    Filters task arguments using a custom callable.

    This provides maximum flexibility for complex filtering logic.
    """

    def __init__(
        self,
        callable_filter: Callable[[dict[str, Any]], bool] | SerializableCallable,
    ) -> None:
        """
        Initialize with a callable filter.

        :param callable_filter: Function that takes arguments and returns a boolean
        """
        if isinstance(callable_filter, SerializableCallable):
            self.callable_filter = callable_filter
        else:
            self.callable_filter = SerializableCallable(callable_filter)

    @cached_property
    def filter_id(self) -> str:
        """
        Generate a unique ID for this argument filter.

        The ID is based on the ID of the callable filter.
        """
        return f"callable_{self.callable_filter.callable_id()}"

    def filter_arguments(self, arguments: dict[str, Any]) -> bool:
        """
        Apply the callable filter to the arguments.

        :param arguments: Task arguments to check
        :return: True if arguments match the filter, False otherwise
        """
        result = self.callable_filter(arguments)
        if not isinstance(result, bool):
            raise ValueError(
                f"Callable filter must return a boolean, got {type(result)}"
            )
        return result

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this argument filter.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized argument filter data
        """
        del app  # Not used
        return {"callable_filter": self.callable_filter.to_json()}

    @classmethod
    def _from_json(
        cls, data: dict[str, Any], app: "Pynenc"
    ) -> "CallableArgumentFilter":
        """
        Create a CallableArgumentFilter instance from parsed JSON data.

        :param data: Dictionary with argument filter data from JSON
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new CallableArgumentFilter instance
        """
        callable_filter = SerializableCallable.from_json(data["callable_filter"])
        return cls(callable_filter)


def create_argument_filter(
    filter_spec: None | dict[str, Any] | Callable[[dict[str, Any]], bool]
) -> ArgumentFilter:
    """
    Factory function to create the appropriate argument filter based on the input type.

    :param filter_spec: Either a dictionary of arguments or a callable function
    :return: An instance of ArgumentFilter
    """
    if filter_spec is None:
        return StaticArgumentFilter({})
    if callable(filter_spec) and not isinstance(filter_spec, dict):
        return CallableArgumentFilter(filter_spec)
    else:
        return StaticArgumentFilter(cast(dict[str, Any], filter_spec))
