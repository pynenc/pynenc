"""
Argument providers for trigger-based task executions.

This module defines the abstractions and implementations for generating
arguments for tasks triggered by various conditions. It enables flexible
mapping from trigger contexts to task arguments.
"""

import json
import logging
from abc import ABC, abstractmethod
from typing import (
    TYPE_CHECKING,
    Any,
    Callable,
    ClassVar,
    Generic,
    Protocol,
    TypeVar,
    cast,
)

from pynenc.arguments import Arguments
from pynenc.trigger.arguments.arguments_common import SerializableCallable
from pynenc.trigger.conditions.base import ConditionContext
from pynenc.util.subclasses import build_class_cache

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.trigger.trigger_context import TriggerContext
# Create a generic type variable for condition contexts
C = TypeVar("C", bound=ConditionContext)
# Create a contravariant type variable for callable context
C_contra = TypeVar("C_contra", bound=ConditionContext, contravariant=True)

logger = logging.getLogger(__name__)


class ArgumentProviderError(Exception):
    """
    Raised when an argument provider cannot generate arguments for a task.

    This can occur when:
    - A context-specific provider can't find a matching context
    - A provider encounters an error while extracting arguments
    - A provider receives an incompatible context type
    """

    def __init__(
        self,
        provider: "ArgumentProvider",
        message: str | None = None,
    ) -> None:
        """
        Initialize with details about the provider failure.

        :param Optional[str] message: Explanation of the error
        :param ArgumentProvider provider: Argument provider that failed
        :param Optional[Exception] cause: Original exception that caused this error, if any
        """
        message = message or "Argument provider failed to generate arguments"
        super().__init__(f"{provider.__class__.__name__}: {message}")


class ArgumentProvider(ABC):
    """
    Base class for argument providers.

    Argument providers generate arguments for tasks based on trigger contexts.
    """

    _argument_provider_class_cache: ClassVar[dict[str, type["ArgumentProvider"]]] = {}
    _cache_initialized: ClassVar[bool] = False

    @classmethod
    def _initialize_class_cache(cls) -> None:
        """
        Initialize the condition class cache by recursively finding all subclasses.
        """
        if cls._cache_initialized:
            return

        cls._argument_provider_class_cache = build_class_cache(cls)
        cls._cache_initialized = True

    @classmethod
    def get_argument_provider_class(
        cls, argument_provider_type: str
    ) -> type["ArgumentProvider"] | None:
        """
        Get an argument provider class by its name from the class cache.

        :param argument_provider_type: Name of the argument provider class to find
        :return: The argument provider class or None if not found
        """
        if not cls._cache_initialized:
            cls._initialize_class_cache()

        return cls._argument_provider_class_cache.get(argument_provider_type)

    @abstractmethod
    def get_arguments(self, trigger_context: "TriggerContext") -> dict[str, Any]:
        """
        Generate arguments for a task based on a trigger context.

        :param TriggerContext trigger_context: Context containing the satisfied conditions
        :return: Dictionary of arguments
        :raises Exception: If the provider fails to generate arguments
        """

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this condition to a JSON string.

        :param app: Pynenc application instance for serializing complex arguments
        :return: JSON string representation of this condition
        """
        data = self._to_json(app)
        return json.dumps({"condition_type": self.__class__.__name__, "data": data})

    @abstractmethod
    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this condition.

        Subclasses must implement this method to handle their specific serialization logic.

        :param app: Pynenc application instance for serializing complex arguments
        :return: Dictionary with serialized condition data
        """
        pass

    @classmethod
    def from_json(cls, json_str: str, app: "Pynenc") -> "ArgumentProvider":
        """
        Create a argument provider instance from a JSON string.

        This is a factory method that instantiates the correct subclass based on the
        argument_provider_type field in the JSON data.

        :param json_str: JSON string containing serialized argument provider data
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of the appropriate TriggerCondition subclass
        :raises ValueError: If the JSON data is invalid or the condition type is unknown
        """
        try:
            data_dict = json.loads(json_str)
            argument_provider_type = data_dict.get("condition_type")
            data = data_dict.get("data", {})

            # Find the appropriate argument provider class using the class cache
            argument_provider_class = cls.get_argument_provider_class(
                argument_provider_type
            )

            if argument_provider_class is None:
                raise ValueError(
                    f"Unknown argument provider type: {argument_provider_type}"
                )

            return argument_provider_class._from_json(data, app)

        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON for argument provider") from e

    @classmethod
    @abstractmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ArgumentProvider":
        """
        Create an argument provider instance from parsed JSON data.

        Each subclass must implement this method to handle its specific deserialization logic.

        :param data: Dictionary with argument provider data from JSON
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of this argument provider class
        :raises ValueError: If the data is invalid for this argument provider type
        """
        pass


class StaticArgumentProvider(ArgumentProvider):
    """
    Provides static arguments independent of trigger context.
    """

    def __init__(self, arguments: dict[str, Any]) -> None:
        """
        Initialize with fixed arguments.

        :param arguments: Static arguments to provide
        """
        self.arguments = Arguments(arguments)

    def get_arguments(self, trigger_context: "TriggerContext") -> dict[str, Any]:
        """
        Return the static arguments.

        :param trigger_context: Ignored for static providers
        :return: The static arguments dictionary
        """
        del trigger_context
        return self.arguments.kwargs.copy()

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        return {"arguments": self.arguments.to_json(app)}

    @classmethod
    def _from_json(
        cls, data: dict[str, Any], app: "Pynenc"
    ) -> "StaticArgumentProvider":
        arguments = Arguments.from_json(app, data.get("arguments", {}))
        return cls(arguments.kwargs)

    def __eq__(self, value: Any) -> bool:
        """
        Check equality with another object.

        :param value: Object to compare with
        :return: True if equal, False otherwise
        """
        if isinstance(value, StaticArgumentProvider):
            return self.arguments == value.arguments
        return False


# Define a Protocol for type-safe callbacks on specific context types
class ContextCallable(Protocol[C_contra]):
    """Protocol for callables that process a specific context type."""

    def __call__(self, context: C_contra) -> dict[str, Any]:
        ...


class ContextTypeArgumentProvider(ArgumentProvider, Generic[C]):
    """
    Generates arguments from a specific context type using a user-provided function.
    """

    def __init__(
        self,
        context_type: type[C],
        callback: ContextCallable[C] | SerializableCallable,
    ) -> None:
        """
        Initialize with context type and callback.

        :param context_type: Type of context this provider handles
        :param callback: Function to extract arguments from the context
        """
        self.context_type = context_type
        if isinstance(callback, SerializableCallable):
            self.callback = callback
        else:
            self.callback = SerializableCallable(callback)

    def get_arguments(self, trigger_context: "TriggerContext") -> dict[str, Any]:
        for valid_condition in trigger_context.valid_conditions.values():
            context = valid_condition.context
            if isinstance(context, self.context_type):
                args = self.callback(cast(C, context))
                return args

        raise ArgumentProviderError(
            self,
            f"Context of type {self.context_type} not found in trigger context",
        )

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        del app
        return {
            "context_type": self.context_type.__name__,
            "callback": self.callback.to_json(),
        }

    @classmethod
    def _from_json(
        cls, data: dict[str, Any], app: "Pynenc"
    ) -> "ContextTypeArgumentProvider":
        context_type_name = data["context_type"]
        context_type_cls = ConditionContext.get_context_class(context_type_name)
        callback = SerializableCallable.from_json(data["callback"])
        return cls(cast(type[C], context_type_cls), callback)


class DirectArgumentProvider(ArgumentProvider):
    """
    Provides complete control over argument generation by giving direct access to the trigger context.
    """

    def __init__(
        self,
        callback: Callable[["TriggerContext"], dict[str, Any]] | SerializableCallable,
    ) -> None:
        """
        Initialize with a callback that processes the entire trigger context.

        :param callback: Function to extract arguments from the trigger context
        :param fallback: If True, ignores failures and returns None; if False, raises exceptions
        """
        if isinstance(callback, SerializableCallable):
            self.callback = callback
        else:
            self.callback = SerializableCallable(callback)

    def get_arguments(self, trigger_context: "TriggerContext") -> dict[str, Any]:
        args = self.callback(trigger_context)
        if not isinstance(args, dict):
            raise ArgumentProviderError(
                self,
                f"Callback must return a dictionary of arguments, got {type(args)}",
            )
        return args

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        del app
        return {
            "callback": self.callback.to_json(),
        }

    @classmethod
    def _from_json(
        cls, data: dict[str, Any], app: "Pynenc"
    ) -> "DirectArgumentProvider":
        callback = SerializableCallable.from_json(data["callback"])
        return cls(callback)


class CompositeArgumentProvider(ArgumentProvider):
    """
    Combines multiple argument providers and merges their results.
    """

    def __init__(self, providers: list[ArgumentProvider]) -> None:
        """
        Initialize with a list of argument providers.

        :param providers: List of argument providers to combine
        """
        self.providers: list[ArgumentProvider] = providers

    def get_arguments(self, trigger_context: "TriggerContext") -> dict[str, Any]:
        args: dict[str, Any] = {}
        failures: list[ArgumentProviderError] = []
        for provider in self.providers:
            try:
                args = provider.get_arguments(trigger_context)
                if isinstance(args, dict):
                    return args
            except ArgumentProviderError as e:
                logger.warning(f"Argument provider failed: {e}")
                failures.append(e)
        if failures:
            message = (
                "Multiple argument providers failed to generate arguments:\n"
                + "\n".join(str(e) for e in failures)
            )
            raise ArgumentProviderError(self, message)
        raise ArgumentProviderError(self)

    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        return {"providers": [p.to_json(app) for p in self.providers]}

    @classmethod
    def _from_json(
        cls, data: dict[str, Any], app: "Pynenc"
    ) -> "CompositeArgumentProvider":
        providers = []
        for provider_data in data["providers"]:
            provider_class = ArgumentProvider.from_json(provider_data, app)
            providers.append(provider_class)
        return cls(providers)
