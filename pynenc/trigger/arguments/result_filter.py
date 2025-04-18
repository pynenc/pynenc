"""
Result filters for trigger-based task executions.

This module defines filters for task results that leverage the existing
argument filtering infrastructure. These filters determine if task results
match certain criteria by adapting them to the argument filter format.
"""

from functools import cached_property
from typing import TYPE_CHECKING, Any, Callable, Protocol, runtime_checkable

from pynenc.trigger.arguments.argument_filters import (
    ArgumentFilter,
    CallableArgumentFilter,
    StaticArgumentFilter,
)
from pynenc.trigger.arguments.arguments_common import SerializableCallable

if TYPE_CHECKING:
    from pynenc.app import Pynenc


@runtime_checkable
class ResultFilterProtocol(Protocol):
    """
    Protocol defining the interface for result filters.

    This protocol specifies the required methods that any result filter must implement,
    regardless of class hierarchy.
    """

    @cached_property
    def filter_id(self) -> str:
        """
        Generate a unique ID for this argument filter.

        The ID is based on the ID of the callable filter.
        """

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this argument filter to a JSON string.

        :param app: Pynenc application instance for serializing complex arguments
        :return: JSON string representation of this argument filter
        """

    def filter_result(self, result: Any) -> bool:
        """
        Determine if a result value satisfies this filter.

        :param result: The result value to check
        :return: True if the result satisfies the filter criteria
        """


class NoResultFilter(ArgumentFilter):
    """This will disable result filtering."""

    @cached_property
    def filter_id(self) -> str:
        return "no_result_filter"

    def filter_arguments(self, arguments: dict[str, Any]) -> bool:
        del arguments
        raise NotImplementedError(
            "filter_arguments is not supported for NoResultFilter"
        )

    def filter_result(self, result: Any) -> bool:
        return True

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        return {}

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "NoResultFilter":
        del data, app
        return cls()


class StaticResultFilter(StaticArgumentFilter):
    """
    Filters task results based on exact matching with an expected value.

    This filter adapts the StaticArgumentFilter to work with result values.
    """

    def __init__(self, expected_result: Any) -> None:
        """
        Initialize with an expected result value.

        :param expected_result: Result value that task results must match
        """
        super().__init__({"result": expected_result})

    @classmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "StaticResultFilter":
        """
        Create a StaticResultFilter from parsed JSON data.

        :param data: Dictionary with filter data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new StaticResultFilter instance
        """
        arg_filter = StaticArgumentFilter._from_json(data, app)
        return cls(arg_filter.arguments.kwargs["result"])

    def filter_result(self, result: Any) -> bool:
        """
        Check if the task result matches the expected value.

        :param result: The result to check
        :return: True if the result matches the expected value, False otherwise
        """
        return super().filter_arguments({"result": result})


class CallableResultFilter(CallableArgumentFilter):
    """
    Filters task results using a custom callable function.

    This filter adapts the CallableArgumentFilter to work with result values.
    """

    def __init__(
        self,
        callable_filter: Callable[[Any], bool] | SerializableCallable,
    ) -> None:
        """
        Initialize with a callable filter for results.

        :param callable_filter: Function that takes a result and returns a boolean
        """
        if isinstance(callable_filter, SerializableCallable):
            self.callable_filter = callable_filter
        else:
            self.callable_filter = SerializableCallable(callable_filter)
        super().__init__(self.callable_filter)

    def filter_arguments(self, arguments: dict[str, Any]) -> bool:
        raise NotImplementedError(
            "filter_arguments is not supported for CallableResultFilter"
        )

    def filter_result(self, result: Any) -> bool:
        result = self.callable_filter(result)
        if not isinstance(result, bool):
            raise ValueError(
                f"Callable filter must return a boolean, got {type(result)}"
            )
        return result


def create_result_filter(
    filter_spec: Any | Callable[[Any], bool]
) -> ResultFilterProtocol:
    """
    Factory function to create the appropriate result filter based on the input type.

    :param filter_spec: Either an expected result value or a callable function
    :return: An instance of ArgumentFilter
    """
    if callable(filter_spec):
        return CallableResultFilter(filter_spec)
    return StaticResultFilter(filter_spec)
