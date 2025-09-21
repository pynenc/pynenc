from pynenc.exceptions import InvocationError


def test_invocation_error_without_message() -> None:
    """
    Test the InvocationError class when no custom message is provided.
    """
    invocation_id = "test_invocation_id"
    error = InvocationError(invocation_id)
    assert str(error) == f"InvocationError({invocation_id})"
    assert error.message is None


def test_invocation_error_with_message() -> None:
    """
    Test the InvocationError class when a custom message is provided.
    """
    invocation_id = "test_invocation_id"
    custom_message = "Custom error message"
    error = InvocationError(invocation_id, message=custom_message)
    assert str(error) == f"InvocationError({invocation_id}): {custom_message}"
    assert error.message == custom_message


def test_to_json_dict() -> None:
    """
    Test the _to_json_dict method of InvocationError.
    """
    invocation_id = "test_invocation_id"
    custom_message = "Custom error message"
    error = InvocationError(invocation_id, message=custom_message)
    error_dict = error._to_json_dict()
    assert error_dict == {"invocation_id": invocation_id, "message": custom_message}


def test_from_json_dict() -> None:
    """
    Test the _from_json_dict method of InvocationError.
    """
    invocation_id = "test_invocation_id"
    custom_message = "Custom error message"
    json_dict = {"invocation_id": invocation_id, "message": custom_message}
    error = InvocationError._from_json_dict(json_dict)
    assert isinstance(error, InvocationError)
    assert error.invocation_id == invocation_id
    assert error.message == custom_message
