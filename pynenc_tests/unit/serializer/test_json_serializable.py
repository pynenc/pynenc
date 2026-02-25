"""
Tests for the JsonSerializable protocol and its integration with JsonSerializer.

Classes used in round-trip tests are defined at module level so that importlib
can locate and reconstruct them during deserialization (qualname resolution requires
the class to be reachable via module attribute lookup).
"""

from enum import Enum, IntEnum

import json

import pytest

from pynenc.serializer.constants import ReservedKeys
from pynenc.serializer.json_serializer import (
    DefaultJSONEncoder,
    JsonSerializable,
    JsonSerializer,
)


# ---------------------------------------------------------------------------
# Module-level Enum types for round-trip tests (importlib must be able to find them)
# ---------------------------------------------------------------------------


class _Color(Enum):
    RED = "red"
    GREEN = "green"


class _Priority(IntEnum):
    LOW = 1
    HIGH = 10


# ---------------------------------------------------------------------------
# Module-level domain objects used in tests
# ---------------------------------------------------------------------------


class _Money:
    """Minimal value object implementing the full JsonSerializable protocol."""

    def __init__(self, amount: float, currency: str) -> None:
        self.amount = amount
        self.currency = currency

    def to_json(self) -> dict:
        return {"amount": self.amount, "currency": self.currency}

    @classmethod
    def from_json(cls, data: dict) -> "_Money":
        return cls(data["amount"], data["currency"])

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _Money):
            return NotImplemented
        return self.amount == other.amount and self.currency == other.currency


class _MultiType:
    """Covers all JSON-primitive field types in one object."""

    def __init__(
        self,
        int_val: int,
        float_val: float,
        str_val: str,
        list_val: list,
        dict_val: dict,
        none_val: None,
    ) -> None:
        self.int_val = int_val
        self.float_val = float_val
        self.str_val = str_val
        self.list_val = list_val
        self.dict_val = dict_val
        self.none_val = none_val

    def to_json(self) -> dict:
        return {
            "int_val": self.int_val,
            "float_val": self.float_val,
            "str_val": self.str_val,
            "list_val": self.list_val,
            "dict_val": self.dict_val,
            "none_val": self.none_val,
        }

    @classmethod
    def from_json(cls, data: dict) -> "_MultiType":
        return cls(
            data["int_val"],
            data["float_val"],
            data["str_val"],
            data["list_val"],
            data["dict_val"],
            data["none_val"],
        )

    def __eq__(self, other: object) -> bool:
        if not isinstance(other, _MultiType):
            return NotImplemented
        return self.to_json() == other.to_json()


class _Outer:
    """Outer class that contains a nested class to test dotted qualname resolution."""

    class _Inner:
        def __init__(self, value: str) -> None:
            self.value = value

        def to_json(self) -> dict:
            return {"value": self.value}

        @classmethod
        def from_json(cls, data: dict) -> "_Outer._Inner":
            return cls(data["value"])

        def __eq__(self, other: object) -> bool:
            if not isinstance(other, _Outer._Inner):
                return NotImplemented
            return self.value == other.value


class _OnlyToJson:
    """Only implements ``to_json``, missing ``from_json`` — must not match the protocol."""

    def to_json(self) -> dict:
        return {"x": 1}


class _OnlyFromJson:
    """Only implements ``from_json``, missing ``__json__`` — must not match the protocol."""

    @classmethod
    def from_json(cls, data: dict) -> "_OnlyFromJson":
        return cls()


# ---------------------------------------------------------------------------
# Protocol conformance tests
# ---------------------------------------------------------------------------


def test_protocol_check_should_recognize_fully_conforming_class() -> None:
    """isinstance check must pass when an object has both required methods."""
    assert isinstance(_Money(1.0, "USD"), JsonSerializable)


def test_protocol_check_should_reject_class_missing_from_json() -> None:
    """An object with only to_json must NOT satisfy the protocol."""
    assert not isinstance(_OnlyToJson(), JsonSerializable)


def test_protocol_check_should_reject_class_missing_to_json() -> None:
    """An object with only from_json must NOT satisfy the protocol."""
    assert not isinstance(_OnlyFromJson(), JsonSerializable)


def test_protocol_check_should_reject_unrelated_object() -> None:
    """Plain objects with no relevant methods must not satisfy the protocol."""
    assert not isinstance(object(), JsonSerializable)


# ---------------------------------------------------------------------------
# Encoder — wire format tests
# ---------------------------------------------------------------------------


def test_encoder_should_embed_module_and_qualname_in_envelope() -> None:
    """Serialized output must carry module and qualname for reconstruction."""
    money = _Money(9.99, "EUR")
    raw = json.dumps(money, cls=DefaultJSONEncoder)
    data = json.loads(raw)

    envelope = data[ReservedKeys.JSON_SERIALIZABLE.value]
    assert envelope["module"] == _Money.__module__
    assert envelope["qualname"] == _Money.__qualname__
    assert envelope["data"] == {"amount": 9.99, "currency": "EUR"}


def test_encoder_should_embed_dotted_qualname_for_nested_class() -> None:
    """Nested class qualname (e.g. ``_Outer._Inner``) must be stored intact."""
    inner = _Outer._Inner("hello")
    raw = json.dumps(inner, cls=DefaultJSONEncoder)
    envelope = json.loads(raw)[ReservedKeys.JSON_SERIALIZABLE.value]

    assert "." in envelope["qualname"]
    assert envelope["qualname"] == _Outer._Inner.__qualname__


def test_encoder_should_raise_type_error_for_object_missing_from_json() -> None:
    """Objects without from_json do not conform and must raise TypeError."""
    with pytest.raises(TypeError):
        json.dumps(_OnlyToJson(), cls=DefaultJSONEncoder)


def test_encoder_should_raise_type_error_for_non_conforming_object() -> None:
    """Arbitrary objects raise TypeError with helpful context (type, value, hint)."""

    class _Arbitrary:
        pass

    with pytest.raises(TypeError, match="JsonSerializable"):
        json.dumps(_Arbitrary(), cls=DefaultJSONEncoder)


# ---------------------------------------------------------------------------
# Enum serialization tests
# ---------------------------------------------------------------------------


def test_encoder_should_embed_enum_envelope_with_module_and_qualname() -> None:
    """Enums are serialized with full type info for round-trip reconstruction."""
    raw = json.dumps(_Color.RED, cls=DefaultJSONEncoder)
    data = json.loads(raw)

    envelope = data[ReservedKeys.ENUM.value]
    assert envelope["module"] == _Color.__module__
    assert envelope["qualname"] == _Color.__qualname__
    assert envelope["value"] == "red"


def test_round_trip_should_reconstruct_str_enum() -> None:
    """String-valued Enum must survive serialization and return the exact member."""
    result = JsonSerializer.deserialize(JsonSerializer.serialize(_Color.GREEN))

    assert isinstance(result, _Color)
    assert result is _Color.GREEN


def test_round_trip_should_reconstruct_int_enum() -> None:
    """IntEnum must survive serialization and return the exact member."""
    result = JsonSerializer.deserialize(JsonSerializer.serialize(_Priority.HIGH))

    assert isinstance(result, _Priority)
    assert result is _Priority.HIGH


def test_round_trip_enum_inside_dict() -> None:
    """Enums nested inside dicts are reconstructed as proper Enum members."""
    data = {"color": _Color.RED, "priority": _Priority.LOW, "count": 3}
    result = JsonSerializer.deserialize(JsonSerializer.serialize(data))

    assert isinstance(result["color"], _Color)
    assert result["color"] is _Color.RED
    assert isinstance(result["priority"], _Priority)
    assert result["priority"] is _Priority.LOW
    assert result["count"] == 3


# ---------------------------------------------------------------------------
# Full round-trip tests via JsonSerializer
# ---------------------------------------------------------------------------


def test_round_trip_should_reconstruct_simple_value_object() -> None:
    """Serialize then deserialize a simple domain object and verify equality."""
    original = _Money(42.5, "GBP")
    serialized = JsonSerializer.serialize(original)
    result = JsonSerializer.deserialize(serialized)

    assert isinstance(result, _Money)
    assert result == original


def test_round_trip_should_preserve_all_primitive_field_types() -> None:
    """Round-trip must faithfully restore int, float, str, list, dict, and None fields."""
    original = _MultiType(1, 3.14, "hello", [1, 2, 3], {"k": "v"}, None)
    result = JsonSerializer.deserialize(JsonSerializer.serialize(original))

    assert isinstance(result, _MultiType)
    assert result == original


def test_round_trip_should_reconstruct_nested_class() -> None:
    """Nested class instances must be reconstructed via dotted qualname traversal."""
    original = _Outer._Inner("world")
    result = JsonSerializer.deserialize(JsonSerializer.serialize(original))

    assert isinstance(result, _Outer._Inner)
    assert result == original


def test_round_trip_should_return_correct_type_not_dict() -> None:
    """Deserialized result must be the original type, never a plain dict."""
    original = _Money(0.01, "JPY")
    result = JsonSerializer.deserialize(JsonSerializer.serialize(original))

    assert not isinstance(result, dict)
    assert type(result) is _Money


def test_round_trip_should_handle_zero_and_empty_values() -> None:
    """Edge-case values (zero amount, empty string currency) must survive round-trip."""
    original = _Money(0.0, "")
    result = JsonSerializer.deserialize(JsonSerializer.serialize(original))

    assert isinstance(result, _Money)
    assert result == original


# ---------------------------------------------------------------------------
# Interaction with other reserved keys
# ---------------------------------------------------------------------------


def test_deserialize_should_return_plain_dict_when_no_reserved_key_present() -> None:
    """Ordinary dicts must pass through deserialization unchanged."""
    data = {"key": "value", "num": 123}
    assert JsonSerializer.deserialize(JsonSerializer.serialize(data)) == data


def test_deserialize_should_still_reconstruct_exceptions_alongside_protocol() -> None:
    """Exception deserialization must be unaffected by the JsonSerializable changes."""
    exc = ValueError("test error")
    result = JsonSerializer.deserialize(JsonSerializer.serialize(exc))

    assert isinstance(result, ValueError)
    assert result.args == exc.args
