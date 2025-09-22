import pytest

from pynenc.util.subclasses import get_all_subclasses, get_subclass


class ParentClass:
    pass


class ChildClass1(ParentClass):
    pass


class ChildClass2(ParentClass):
    pass


class GrandChildClass1(ChildClass1):
    pass


class GrandChildClass2(ChildClass2):
    pass


def test_get_all_subclasses() -> None:
    subclasses = get_all_subclasses(ParentClass)
    assert set(subclasses) == {
        ChildClass1,
        ChildClass2,
        GrandChildClass1,
        GrandChildClass2,
    }


def test_get_subclass_valid() -> None:
    subclass = get_subclass(ParentClass, "GrandChildClass1")
    assert subclass is GrandChildClass1


def test_get_subclass_invalid() -> None:
    with pytest.raises(ValueError):
        get_subclass(ParentClass, "UnknownClass")
