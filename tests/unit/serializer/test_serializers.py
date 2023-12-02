from typing import TYPE_CHECKING, Type

import pytest

from pynenc.serializer.base_serializer import BaseSerializer
from pynenc.util.subclasses import get_all_subclasses

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest
    from _pytest.python import Metafunc


def pytest_generate_tests(metafunc: "Metafunc") -> None:
    subclasses = get_all_subclasses(BaseSerializer)  # type: ignore # mypy issue #4717
    if "serializer_class" in metafunc.fixturenames:
        metafunc.parametrize("serializer_class", subclasses, indirect=True)


@pytest.fixture
def serializer_class(request: "FixtureRequest") -> Type[BaseSerializer]:
    return request.param


def test_serialize_deserialize_integers(serializer_class: Type[BaseSerializer]) -> None:
    obj = 42
    serialized = serializer_class.serialize(obj)
    assert serializer_class.deserialize(serialized) == obj


def test_serialize_deserialize_strings(serializer_class: Type[BaseSerializer]) -> None:
    obj = "hello"
    serialized = serializer_class.serialize(obj)
    assert serializer_class.deserialize(serialized) == obj


def test_serialize_deserialize_lists(serializer_class: Type[BaseSerializer]) -> None:
    obj = [1, 2, 3]
    serialized = serializer_class.serialize(obj)
    assert serializer_class.deserialize(serialized) == obj


def test_serialize_deserialize_dicts(serializer_class: Type[BaseSerializer]) -> None:
    obj = {"key": "value"}
    serialized = serializer_class.serialize(obj)
    assert serializer_class.deserialize(serialized) == obj


def test_serialize_deserialize_exceptions(
    serializer_class: Type[BaseSerializer],
) -> None:
    obj = ValueError("An exception")
    serialized = serializer_class.serialize(obj)
    deserialized = serializer_class.deserialize(serialized)
    assert isinstance(deserialized, ValueError)
    assert deserialized.args == obj.args
