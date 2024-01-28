import builtins
import json
from enum import StrEnum
from typing import Any

from pynenc.serializer.base_serializer import BaseSerializer


class ReservedKeys(StrEnum):
    """
    Defines keys reserved for internal use in the serialization process.

    This enumeration ensures that specific keys are uniquely identifiable and not confused with user-defined data.

    :cvar ERROR: Reserved key for storing error information in serialized data.
    """

    ERROR = "__pynenc__std_py_exc__"


class DefaultJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder that extends the default JSONEncoder to handle special object types.

    This encoder specifically handles the serialization of Python exceptions,
    turning them into a JSON-friendly format.
    """

    def default(self, obj: Any) -> Any:
        """
        Overrides the default method to provide custom serialization logic.

        :param Any obj: The object to be serialized.
        :returns:
            A serialized representation of 'obj', particularly for Exception objects.
            For other types, it falls back to the superclass's default method.
        """
        if isinstance(obj, Exception):
            return {
                ReservedKeys.ERROR.value: {
                    "type": type(obj).__name__,
                    "args": obj.args,
                    "message": str(obj),
                }
            }
        return super().default(obj)


class JsonSerializer(BaseSerializer):
    """
    Implements the BaseSerializer for JSON serialization using the custom DefaultJSONEncoder.

    This class provides methods to serialize and deserialize objects to and from JSON strings,
    with special handling for Python exceptions.
    """

    @staticmethod
    def serialize(obj: Any) -> str:
        """
        Serializes an object into a JSON string.

        :param Any obj: The object to serialize.
        :returns: A JSON string representation of 'obj'.
        """
        return json.dumps(obj, cls=DefaultJSONEncoder)

    @staticmethod
    def deserialize(obj: str) -> Any:
        """
        Deserializes a JSON string back into an object.

        If the string represents a serialized Python exception, it reconstructs the exception object.

        :param str obj: The JSON string to deserialize.
        :returns: The deserialized object, which could be a Python exception if appropriate.
        """
        data = json.loads(obj)
        if isinstance(data, dict):
            if error_data := data.get(ReservedKeys.ERROR.value):
                error_type = error_data["type"]
                error_args = error_data["args"]
                if hasattr(builtins, error_type):
                    return getattr(builtins, error_type)(*error_args)
        return data
