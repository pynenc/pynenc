import builtins
import importlib
import json
from typing import Any, Protocol, runtime_checkable

from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.serializer.constants import ReservedKeys


@runtime_checkable
class JsonSerializable(Protocol):
    """Protocol for objects that support full round-trip JSON serialization.

    Implement **both** ``to_json()`` and ``from_json()`` on your class.
    :class:`DefaultJSONEncoder` will embed the class's module and qualified name
    alongside the serialized data so that :class:`JsonSerializer` can reconstruct
    the original object on deserialization.

    Example::

        from pynenc.serializer.json_serializer import JsonSerializable

        class Money:
            def __init__(self, amount: float, currency: str) -> None:
                self.amount = amount
                self.currency = currency

            def to_json(self) -> dict:
                return {"amount": self.amount, "currency": self.currency}

            @classmethod
            def from_json(cls, data: dict) -> "Money":
                return cls(data["amount"], data["currency"])

    Any task argument or return value that implements this protocol will be
    transparently serialized **and deserialized** when using :class:`JsonSerializer`.
    """

    def to_json(self) -> Any:
        """Return a JSON-serializable representation of this object."""
        ...

    @classmethod
    def from_json(cls, data: Any) -> Any:
        """Reconstruct an instance from the value returned by ``__json__``."""
        ...


class DefaultJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder that extends the default JSONEncoder to handle special object types.

    This encoder handles:

    - **Python exceptions** — serialized to a dict with ``type``, ``args``, and ``message``.
        - **Objects implementing** :class:`JsonSerializable` — any object with a ``to_json()``
      method is serialized by calling that method.  This is the recommended way for
      application code to make domain objects compatible with Pynenc's JSON serializer
      without subclassing or patching the encoder.
    """

    def default(self, obj: Any) -> Any:
        """
        Overrides the default method to provide custom serialization logic.

        Resolution order:

        1. **Exception** — serialized to ``{ReservedKeys.ERROR: {...}}``.
        2. **JsonSerializable** — any object that implements ``to_json()``
           is serialized by calling that method.
        3. All other types fall back to :meth:`json.JSONEncoder.default`,
           which raises ``TypeError``.

        :param Any obj: The object to be serialized.
        :returns: A JSON-serializable representation of ``obj``.
        """
        if isinstance(obj, Exception):
            return {
                ReservedKeys.ERROR.value: {
                    "type": type(obj).__name__,
                    "args": obj.args,
                    "message": str(obj),
                }
            }
        if isinstance(obj, JsonSerializable):
            cls_ = type(obj)
            return {
                ReservedKeys.JSON_SERIALIZABLE.value: {
                    "module": cls_.__module__,
                    "qualname": cls_.__qualname__,
                    "data": obj.to_json(),
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
            if js_data := data.get(ReservedKeys.JSON_SERIALIZABLE.value):
                module = importlib.import_module(js_data["module"])
                # qualname may be "Outer.Inner" for nested classes
                cls_ = module
                for part in js_data["qualname"].split("."):
                    cls_ = getattr(cls_, part)
                return cls_.from_json(js_data["data"])  # type: ignore[union-attr]
        return data
