from enum import StrEnum

import pytest

from pynenc.conf.config_task import TaskOptionsJSONEncoder


class TestStrEnum(StrEnum):
    OPTION1 = "Value1"
    OPTION2 = "Value2"


def test_json_encoder_with_exception_class() -> None:
    """Test JSON encoding of an exception class."""

    class MyException(Exception):
        pass

    encoder = TaskOptionsJSONEncoder()
    result = encoder.default(MyException)
    assert result == f"{MyException.__module__}.{MyException.__name__}"


def test_json_encoder_with_strenum() -> None:
    """Test JSON encoding of an TestStrEnum instance."""
    encoder = TaskOptionsJSONEncoder()
    result = encoder.default(TestStrEnum.OPTION1)
    assert result == "Value1"


def test_json_encoder_with_unhandled_type() -> None:
    """Test JSON encoding with a type not specially handled."""
    encoder = TaskOptionsJSONEncoder()
    with pytest.raises(TypeError):
        encoder.default(complex(1, 1))
