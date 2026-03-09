import builtins
import importlib
import json
from enum import Enum
from typing import Any, Protocol, runtime_checkable, TypeVar

from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.serializer.constants import ReservedKeys


TJsonSerializable = TypeVar("TJsonSerializable", bound="JsonSerializable")


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
        """Reconstruct an instance from the value returned by ``to_json``."""
        ...


class DefaultJSONEncoder(json.JSONEncoder):
    """
    A custom JSON encoder that extends the default JSONEncoder to handle special object types.

    This encoder handles:

    - **Enums** — serialized with module, qualname, and value for full round-trip reconstruction.
    - **Builtin exceptions** (e.g. ``ValueError``) — serialized with type, args, and message
      under :attr:`ReservedKeys.ERROR`.
    - **Custom (non-builtin) exceptions** — serialized with module, qualname, args, and message
      under :attr:`ReservedKeys.CLIENT_EXCEPTION` so they can be fully reconstructed.
    - **Objects implementing** :class:`JsonSerializable` — any object with a ``to_json()``
      method is serialized by calling that method.  This is the recommended way for
      application code to make domain objects compatible with Pynenc's JSON serializer
      without subclassing or patching the encoder.
    """

    def default(self, obj: Any) -> Any:
        """
        Overrides the default method to provide custom serialization logic.

        Resolution order:

        1. **Enum** — serialized with module, qualname, and value for reconstruction.
        2. **Builtin Exception** — serialized to ``{ReservedKeys.ERROR: {type, args, message}}``.
        3. **Custom Exception** — serialized to ``{ReservedKeys.CLIENT_EXCEPTION: {module, qualname, args, message}}``
           so that non-builtin exceptions (user-defined) can be fully round-tripped.
        4. **JsonSerializable** — any object that implements ``to_json()``
           is serialized by calling that method.
        5. All other types raise ``TypeError`` with a descriptive message
           including the object's type and a truncated repr.

        :param Any obj: The object to be serialized.
        :returns: A JSON-serializable representation of ``obj``.
        :raises TypeError: If the object is not serializable.
        """
        json_cls: type
        if isinstance(obj, Enum):
            json_cls = type(obj)
            return {
                ReservedKeys.ENUM.value: {
                    "module": json_cls.__module__,
                    "qualname": json_cls.__qualname__,
                    "value": obj.value,
                }
            }
        if isinstance(obj, Exception):
            json_cls = type(obj)
            if json_cls.__module__ == "builtins":
                return {
                    ReservedKeys.ERROR.value: {
                        "type": json_cls.__name__,
                        "args": obj.args,
                        "message": str(obj),
                    }
                }
            return {
                ReservedKeys.CLIENT_EXCEPTION.value: {
                    "module": json_cls.__module__,
                    "qualname": json_cls.__qualname__,
                    "args": obj.args,
                    "message": str(obj),
                }
            }
        if isinstance(obj, JsonSerializable):
            json_cls = type(obj)
            return {
                ReservedKeys.JSON_SERIALIZABLE.value: {
                    "module": json_cls.__module__,
                    "qualname": json_cls.__qualname__,
                    "data": obj.to_json(),
                }
            }
        raise TypeError(
            f"Object of type {type(obj).__module__}.{type(obj).__qualname__} "
            f"is not JSON serializable. "
            f"Value (truncated): {repr(obj)[:200]}. "
            f"Consider implementing the JsonSerializable protocol (to_json/from_json) "
            f"or switching to JsonPickleSerializer."
        )


def _preprocess_for_json(obj: Any) -> Any:
    """Recursively convert Enum instances to envelope dicts before encoding.

    IntEnum and StrEnum subclasses are natively serializable by json, so
    ``JSONEncoder.default()`` is never called for them.  This pre-pass
    ensures they get the same envelope treatment as regular Enums.
    """
    if isinstance(obj, Enum):
        cls_ = type(obj)
        return {
            ReservedKeys.ENUM.value: {
                "module": cls_.__module__,
                "qualname": cls_.__qualname__,
                "value": obj.value,
            }
        }
    if isinstance(obj, dict):
        return {k: _preprocess_for_json(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_preprocess_for_json(item) for item in obj]
    return obj


def _resolve_class(module_name: str, qualname: str) -> type:
    """Import a module and traverse a dotted qualname to find the class."""
    module = importlib.import_module(module_name)
    cls_: Any = module
    for part in qualname.split("."):
        cls_ = getattr(cls_, part)
    return cls_


def _reconstruct_from_json(data: Any) -> TJsonSerializable | Any:
    """Recursively reconstruct typed objects from reserved-key envelopes."""
    if isinstance(data, dict):
        if error_data := data.get(ReservedKeys.ERROR.value):
            error_type = error_data["type"]
            error_args = error_data["args"]
            if hasattr(builtins, error_type):
                return getattr(builtins, error_type)(*error_args)
            # Fallback for legacy data serialized without module/qualname
            return RuntimeError(
                f"{error_type}: {error_data.get('message', str(error_args))}"
            )
        if client_exc_data := data.get(ReservedKeys.CLIENT_EXCEPTION.value):
            try:
                exc_cls = _resolve_class(
                    client_exc_data["module"], client_exc_data["qualname"]
                )
                return exc_cls(*client_exc_data["args"])
            except Exception:
                return RuntimeError(
                    f"{client_exc_data['qualname']}: {client_exc_data['message']}"
                )
        if js_data := data.get(ReservedKeys.JSON_SERIALIZABLE.value):
            json_cls: type[TJsonSerializable] = _resolve_class(
                js_data["module"], js_data["qualname"]
            )
            return json_cls.from_json(js_data["data"])
        if enum_data := data.get(ReservedKeys.ENUM.value):
            enum_cls: type[Enum] = _resolve_class(
                enum_data["module"], enum_data["qualname"]
            )
            return enum_cls(enum_data["value"])
        return {k: _reconstruct_from_json(v) for k, v in data.items()}
    if isinstance(data, list):
        return [_reconstruct_from_json(item) for item in data]
    return data


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

        Enum instances (including IntEnum/StrEnum) are preprocessed into
        envelope dicts so they survive the round-trip.

        :param Any obj: The object to serialize.
        :returns: A JSON string representation of 'obj'.
        """
        return json.dumps(_preprocess_for_json(obj), cls=DefaultJSONEncoder)

    @staticmethod
    def deserialize(obj: str) -> Any:
        """
        Deserializes a JSON string back into an object.

        Recursively reconstructs objects from reserved-key envelopes
        (exceptions, JsonSerializable, Enum).

        :param str obj: The JSON string to deserialize.
        :returns: The deserialized object.
        """
        return _reconstruct_from_json(json.loads(obj))
