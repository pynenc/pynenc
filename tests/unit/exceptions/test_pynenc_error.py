import json

import pytest

from pynenc.exceptions import PynencError


class ExampleError(PynencError):
    def __init__(self, message: str):
        super().__init__(message)
        self.message = message


def test_to_json_dict() -> None:
    error = ExampleError("Test error")
    expected_dict = {"message": "Test error"}
    assert error._to_json_dict() == expected_dict


def test_from_json_dict() -> None:
    json_dict = {"message": "Test error"}
    error = ExampleError._from_json_dict(json_dict)
    assert isinstance(error, ExampleError)
    assert error.message == "Test error"


def test_to_json() -> None:
    error = ExampleError("Test error")
    expected_json = json.dumps({"message": "Test error"})
    assert error.to_json() == expected_json


def test_from_json() -> None:
    error_name = "ExampleError"
    serialized = json.dumps({"message": "Test error"})
    error = PynencError.from_json(error_name, serialized)
    assert isinstance(error, ExampleError)
    assert error.message == "Test error"


def test_from_json_same_class() -> None:
    serialized = json.dumps({})
    error = PynencError.from_json("PynencError", serialized)
    assert isinstance(error, PynencError)


def test_from_json_unknown_error() -> None:
    with pytest.raises(ValueError) as exc_info:
        PynencError.from_json("UnknownError", "{}")
    assert "Unknown error type: UnknownError" in str(exc_info.value)
