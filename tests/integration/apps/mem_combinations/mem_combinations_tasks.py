from typing import Any

from tests.conftest import MockPynenc

mock_app = MockPynenc()


@mock_app.task
def sum(x: int, y: int) -> int:
    return x + y


@mock_app.task
def cycle_start() -> None:
    _ = cycle_end().result


@mock_app.task
def cycle_end() -> None:
    _ = cycle_start().result


@mock_app.task
def raise_exception() -> Any:
    raise ValueError("test")


@mock_app.task
def get_text() -> str:
    return "example"


@mock_app.task
def get_upper() -> str:
    return get_text().result.upper()


@mock_app.task
def direct_cycle() -> str:
    invocation = direct_cycle()
    return invocation.result.upper()
