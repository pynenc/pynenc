from abc import ABC, abstractmethod
from typing import Any


class BaseSerializer(ABC):
    """
    BaseSerializer is an abstract class that defines a standard interface for serialization and deserialization.

    It's designed to be extended for specific serialization formats like JSON, Pickle, or custom formats as needed.
    Implementers of this class need to provide concrete methods for serialize and deserialize operations.
    """

    @staticmethod
    @abstractmethod
    def serialize(obj: Any) -> str:
        """
        Serializes the given object into a string representation.

        This method needs to be implemented by subclasses to convert various data types into a serialized string format.
        It's useful for storing or transmitting data, especially in distributed systems.

        :param obj: The object to be serialized.
        :return: A string representation of the serialized object.
        """

    @staticmethod
    @abstractmethod
    def deserialize(obj: str) -> Any:
        """
        Deserializes the given string back into an object.

        This method should be implemented by subclasses to reconstruct an object from its serialized string form.
        It's particularly important for retrieving or receiving data in its original form.

        :param obj: The string representation of the object to be deserialized.
        :return: The deserialized object.
        """
