from pynenc.exceptions import InvocationConcurrencyWithDifferentArgumentsError
from tests.conftest import MockPynenc

app = MockPynenc()


@app.task
def add(x: int, y: int) -> int:
    return x + y


def test_format_difference() -> None:
    """
    Test the format_difference method of InvocationConcurrencyWithDifferentArgumentsError.
    """
    invocation1 = add(1, 2)
    invocation2 = add(3, 4)
    error_message = InvocationConcurrencyWithDifferentArgumentsError.format_difference(
        existing_call=invocation1.call, new_call=invocation2.call
    )
    assert "Original: {'x': 1, 'y': 2}" in error_message
    assert "Updated: {'x': 3, 'y': 4}" in error_message
    assert "Changes:" in error_message


def test_from_call_mismatch() -> None:
    """
    Test the from_call_mismatch method of InvocationConcurrencyWithDifferentArgumentsError.
    """
    invocation1 = add(1, 2)
    invocation2 = add(3, 4)
    error = InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
        existing_invocation=invocation1, new_call=invocation2.call
    )
    assert error.existing_invocation_id == invocation1.invocation_id
    assert error.new_call_id == invocation2.call.call_id
    assert error.task_id == invocation1.task.task_id
    assert "x: 1 -> 3" in str(error)
    assert "y: 2 -> 4" in str(error)


def test_to_json_and_from_json() -> None:
    """
    Test the _to_json_dict and _from_json_dict methods of InvocationConcurrencyWithDifferentArgumentsError.
    """
    invocation1 = add(1, 2)
    invocation2 = add(3, 4)
    error = InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
        existing_invocation=invocation1, new_call=invocation2.call
    )

    error_dict = error._to_json_dict()
    new_error = InvocationConcurrencyWithDifferentArgumentsError._from_json_dict(
        error_dict
    )
    assert error_dict == {
        "task_id": invocation1.task.task_id,
        "existing_invocation_id": invocation1.invocation_id,
        "new_call_id": invocation2.call.call_id,
        "diff": error.diff,
        "message": None,
    }
    assert str(new_error) == str(error)


def test_str_without_message() -> None:
    """
    Test the __str__ method of InvocationConcurrencyWithDifferentArgumentsError when there is no custom message.
    """
    invocation1 = add(1, 2)
    invocation2 = add(3, 4)
    error = InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
        existing_invocation=invocation1, new_call=invocation2.call
    )
    assert (
        str(error)
        == f"InvocationConcurrencyWithDifferentArgumentsError({invocation1.task.task_id})\n{error.diff}"
    )


def test_str_with_message() -> None:
    """
    Test the __str__ method of InvocationConcurrencyWithDifferentArgumentsError when there is a custom message.
    """
    invocation1 = add(1, 2)
    invocation2 = add(3, 4)
    custom_message = "Custom error message"
    error = InvocationConcurrencyWithDifferentArgumentsError.from_call_mismatch(
        existing_invocation=invocation1,
        new_call=invocation2.call,
        message=custom_message,
    )
    assert (
        str(error)
        == f"InvocationConcurrencyWithDifferentArgumentsError({invocation1.task.task_id}) {custom_message}\n{error.diff}"
    )


def test_format_difference_with_removed_keys() -> None:
    """
    Test the format_difference method of InvocationConcurrencyWithDifferentArgumentsError
    when some keys are removed in the new arguments.
    """
    existing_call = add(0, 0).call
    new_call = add(1, 1).call
    existing_call.arguments.kwargs = {"x": 1, "y": 2, "z": 3}
    new_call.arguments.kwargs = {"x": 2}
    diff = InvocationConcurrencyWithDifferentArgumentsError.format_difference(
        existing_call, new_call
    )
    assert "x: 1" in diff
    assert "y: Removed" in diff
    assert "z: Removed" in diff
    assert "Added" not in diff


def test_format_difference_with_added_keys() -> None:
    """
    Test the format_difference method of InvocationConcurrencyWithDifferentArgumentsError
    when some keys are added in the new arguments.
    """
    existing_call = add(0, 0).call
    new_call = add(1, 1).call
    existing_call.arguments.kwargs = {"x": 1}
    new_call.arguments.kwargs = {"x": 1, "y": 2, "z": 3}
    diff = InvocationConcurrencyWithDifferentArgumentsError.format_difference(
        existing_call, new_call
    )
    assert "x:" not in diff
    assert "y: Added" in diff
    assert "z: Added" in diff
    assert "Removed" not in diff
