"""
Base class for all trigger conditions.

This module defines the core abstractions for condition-based triggering in Pynenc.
Trigger conditions are used to evaluate whether a task should be triggered
based on specific criteria such as task status, time schedules, or custom events.
"""

import json
from abc import ABC, abstractmethod
from dataclasses import dataclass, field
from datetime import datetime
from typing import TYPE_CHECKING, Any, ClassVar, Generic, TypeVar, cast

from pynenc.util.subclasses import build_class_cache

if TYPE_CHECKING:
    from ...app import Pynenc


@dataclass
class ConditionContext(ABC):
    """
    Base class for condition contexts.

    Contains the minimum data required for condition evaluation and
    provides the foundation for type-specific context classes.
    """

    timestamp: datetime = field(default_factory=datetime.now, init=False)

    # Class cache for context type lookup (each subclass should appear here)
    _context_class_cache: ClassVar[dict[str, type["ConditionContext"]]] = {}
    _cache_initialized: ClassVar[bool] = False

    @classmethod
    def _initialize_class_cache(cls) -> None:
        """
        Initialize the context class cache by recursively finding all subclasses.
        """
        if cls._cache_initialized:
            return

        cls._context_class_cache = build_class_cache(cls)
        cls._cache_initialized = True

    @classmethod
    def get_context_class(cls, context_type: str) -> type["ConditionContext"]:
        """
        Get a context class by its name from the class cache.

        :param context_type: Name of the context class to find
        :return: The context class
        """
        if not cls._cache_initialized:
            cls._initialize_class_cache()
        return cls._context_class_cache[context_type]

    @property
    @abstractmethod
    def context_id(self) -> str:
        """
        Generate a stable, unique ID for this context based on its parameters.

        :return: A string ID uniquely identifying this context
        """

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this context to a JSON string.

        :param app: Pynenc application instance
        :return: JSON string representation
        """
        data = self._to_json(app)
        return json.dumps(
            {
                "context_type": self.__class__.__name__,
                "data": data,
                "timestamp": self.timestamp.isoformat(),
            }
        )

    @abstractmethod
    def _to_json(self, app: "Pynenc") -> dict[str, Any]:
        """
        Create a serializable representation of this context.

        Subclasses must implement this method to handle their specific serialization logic.

        :param app: Pynenc application instance
        :return: Dictionary with serialized context data
        """
        pass

    @classmethod
    def from_json(cls, json_str: str, app: "Pynenc") -> "ConditionContext":
        """
        Create a context instance from a JSON string.

        :param json_str: JSON string containing serialized context
        :param app: Pynenc application instance
        :return: A new instance of the appropriate ConditionContext subclass
        :raises ValueError: If the JSON data is invalid
        """
        # Initialize class cache if needed
        if not cls._cache_initialized:
            cls._initialize_class_cache()

        data = json.loads(json_str)
        context_type = data.get("context_type")
        context_data = data.get("data", {})

        context_class = cls.get_context_class(context_type)
        instance = context_class._from_json(context_data, app)

        if "timestamp" in data:
            try:
                instance.timestamp = datetime.fromisoformat(data["timestamp"])
            except (ValueError, TypeError) as e:
                raise ValueError(
                    f"Invalid timestamp format: {data['timestamp']}"
                ) from e

        return instance

    @classmethod
    @abstractmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "ConditionContext":
        """
        Create a context instance from parsed JSON data.

        Subclasses must implement this method to handle their specific deserialization logic.

        :param data: Dictionary with context data
        :param app: Pynenc application instance
        :return: A new instance of this context class
        """
        pass


# Generic type variable for condition contexts
C = TypeVar("C", bound=ConditionContext)


class TriggerCondition(Generic[C], ABC):
    """
    Base class for all trigger conditions.

    A trigger condition evaluates whether a task should be triggered
    based on specific criteria and a matching context type.
    """

    # Class variable for context type - must be defined by subclasses
    context_type: ClassVar[type[ConditionContext]]

    # Class cache for condition type lookup
    _condition_class_cache: ClassVar[dict[str, type["TriggerCondition"]]] = {}
    _cache_initialized: ClassVar[bool] = False

    @classmethod
    def _initialize_class_cache(cls) -> None:
        """
        Initialize the condition class cache by recursively finding all subclasses.
        """
        if cls._cache_initialized:
            return

        cls._condition_class_cache = build_class_cache(cls)
        cls._cache_initialized = True

    @classmethod
    def get_condition_class(
        cls, condition_type: str
    ) -> type["TriggerCondition"] | None:
        """
        Get a condition class by its name from the class cache.

        :param condition_type: Name of the condition class to find
        :return: The condition class or None if not found
        """
        if not cls._cache_initialized:
            cls._initialize_class_cache()

        return cls._condition_class_cache.get(condition_type)

    @abstractmethod
    def get_source_task_ids(self) -> set[str]:
        """
        Get the ID of the task this condition is sourced from, if any.

        Some conditions directly monitor specific tasks (like StatusCondition or ResultCondition).
        This method identifies if this condition is monitoring a specific source task.

        :return: The ID of the source task, or None if this condition is not task-specific
        """

    @property
    @abstractmethod
    def condition_id(self) -> str:
        """
        Generate a stable, unique ID for this condition based on its parameters.

        :return: A string ID uniquely identifying this condition
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
    def from_json(cls, json_str: str, app: "Pynenc") -> "TriggerCondition":
        """
        Create a condition instance from a JSON string.

        This is a factory method that instantiates the correct subclass based on the
        condition_type field in the JSON data.

        :param json_str: JSON string containing serialized condition
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of the appropriate TriggerCondition subclass
        :raises ValueError: If the JSON data is invalid or the condition type is unknown
        """
        try:
            data_dict = json.loads(json_str)
            condition_type = data_dict.get("condition_type")
            condition_data = data_dict.get("data", {})

            # Find the appropriate condition class using the class cache
            condition_class = cls.get_condition_class(condition_type)

            if condition_class is None:
                raise ValueError(f"Unknown condition type: {condition_type}")

            return condition_class._from_json(condition_data, app)

        except json.JSONDecodeError as e:
            raise ValueError("Invalid JSON for condition") from e

    @classmethod
    @abstractmethod
    def _from_json(cls, data: dict[str, Any], app: "Pynenc") -> "TriggerCondition":
        """
        Create a condition instance from parsed JSON data.

        Each subclass must implement this method to handle its specific deserialization logic.

        :param data: Dictionary with condition data from JSON
        :param app: Pynenc application instance for deserializing complex arguments
        :return: A new instance of this condition class
        :raises ValueError: If the data is invalid for this condition type
        """
        pass

    def is_satisfied_by(self, context: ConditionContext) -> bool:
        """
        Check if this condition is satisfied by the given context.

        First validates if the context is of the expected type,
        then delegates to the type-specific implementation.

        :param context: Context to evaluate against
        :return: True if the condition is satisfied, False otherwise
        """
        if not isinstance(context, self.context_type):
            return False
        return self._is_satisfied_by(cast(C, context))

    @abstractmethod
    def _is_satisfied_by(self, context: C) -> bool:
        """
        Type-specific implementation of condition satisfaction.

        This method is called by is_satisfied_by after type validation.

        :param context: Context of the correct type for this condition
        :return: True if the condition is satisfied, False otherwise
        """
        pass

    def affects_task(self, task_id: str) -> bool:
        """
        Check if this condition is affected by a specific task.

        Default implementation returns False as most conditions
        are not directly affected by specific tasks.

        :param task_id: ID of the task to check
        :return: True if the condition is affected by the task
        """
        return False


class ValidCondition:
    """
    Represents a satisfied condition with its associated context.

    This class pairs a TriggerCondition with the specific ConditionContext
    that caused it to be satisfied, creating a "valid condition" that
    can be used to evaluate triggers.
    """

    def __init__(self, condition: TriggerCondition, context: ConditionContext) -> None:
        """
        Create a valid condition.

        :param condition: The satisfied trigger condition
        :param context: The context that satisfied the condition
        """
        self.condition = condition
        self.context = context

    @property
    def valid_condition_id(self) -> str:
        """Get the ID of the underlying condition and context."""
        return f"valid_condition_{self.condition.condition_id}_context_{self.context.context_id}"

    def to_json(self, app: "Pynenc") -> str:
        """
        Serialize this valid condition to a JSON string.

        :param app: Pynenc application instance
        :return: JSON string representation
        """
        data = {
            "condition": self.condition.to_json(app),
            "context": self.context.to_json(app),
        }
        return json.dumps(data)

    @classmethod
    def from_json(cls, json_str: str, app: "Pynenc") -> "ValidCondition":
        """
        Create a valid condition instance from a JSON string.

        :param json_str: JSON string containing serialized valid condition
        :param app: Pynenc application instance
        :return: A new ValidCondition instance
        """
        data = json.loads(json_str)

        condition = TriggerCondition.from_json(data["condition"], app)
        context = ConditionContext.from_json(data["context"], app)

        return cls(condition=condition, context=context)
