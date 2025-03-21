from pynenc.builder import PynencBuilder
from pynenc.conf.config_task import ConcurrencyControlType

app = (
    PynencBuilder()
    .concurrency_control(
        running_concurrency="TASK",
        registration_concurrency=ConcurrencyControlType.ARGUMENTS,
    )
    .build()
)


@app.task
def test_task() -> None:
    pass


def test_concurrency_control() -> None:
    """Test concurrency_control configuration"""

    assert test_task.conf.running_concurrency == ConcurrencyControlType.TASK
    assert test_task.conf.registration_concurrency == ConcurrencyControlType.ARGUMENTS


app2 = (
    PynencBuilder()
    .concurrency_control(
        running_concurrency=ConcurrencyControlType.TASK,
        registration_concurrency="ARGUMENTS",
    )
    .build()
)


@app2.task
def test_task2() -> None:
    pass


def test_concurrency_control2() -> None:
    """Test concurrency_control configuration"""

    assert test_task2.conf.running_concurrency == ConcurrencyControlType.TASK
    assert test_task2.conf.registration_concurrency == ConcurrencyControlType.ARGUMENTS
