from pynenc_tests.conftest import MockPynenc
from pynenc.conf.config_task import ConcurrencyControlType

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None: ...


@mock_app.task
def dummy_sum(x: int, y: int) -> int:
    return x + y


@mock_app.task
def dummy_concat(arg0: str, arg1: str) -> str:
    return f"{arg0}:{arg1}"


@mock_app.task
def dummy_mirror(arg: str) -> str:
    return arg


@mock_app.task
def dummy_key_arg(key: str, arg: str) -> str:
    return f"{key}:{arg}"


@mock_app.task(
    running_concurrency=ConcurrencyControlType.KEYS,
    key_arguments=("key",),
)
def dummy_running_key_arg(key: str, arg: str) -> str:
    return f"{key}:{arg}"
