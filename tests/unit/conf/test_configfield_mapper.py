from typing import Any, Type

import pytest

from pynenc.conf import config_base


def test_default_mapper_str() -> None:
    """test the default config field mapper for str"""
    res = config_base.default_config_field_mapper(0, str)
    assert res == "0"
    assert isinstance(res, str)


def test_default_mapper_int() -> None:
    """test the default config field mapper for int"""
    res = config_base.default_config_field_mapper("0", int)
    assert res == 0
    assert isinstance(res, int)


def test_default_mapper_float() -> None:
    """test the default config field mapper for float"""
    res = config_base.default_config_field_mapper(0, float)
    assert res == 0.0
    assert isinstance(res, float)


def test_default_mapper_set() -> None:
    """test the default config field mapper for set"""
    res = config_base.default_config_field_mapper([0, 1, 1], set)
    assert res == {0, 1}
    assert isinstance(res, set)


def test_other_mapper() -> None:
    def other_mapper(value: Any, expected_type: Type) -> Any:
        """mapper that parse tuples to int, otherwise default"""
        if isinstance(value, tuple):
            return -13
        return config_base.default_config_field_mapper(value, expected_type)

    class ConfTest(config_base.ConfigBase):
        cf = config_base.ConfigField(0, mapper=other_mapper)

    # check expected behaviour of default config field mapper
    conf = ConfTest()
    assert conf.cf == 0
    conf.cf = "1"
    assert isinstance(conf.cf, int)
    assert conf.cf == 1

    # test 'other_mapper" tuple mapper
    conf.cf = ("asdf", 8)
    assert isinstance(conf.cf, int)
    assert conf.cf == -13

    # test type error mapping a list to int
    with pytest.raises(TypeError):
        conf.cf = [0, "a"]
