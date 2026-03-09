from pynenc import Pynenc
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


@app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def test_overwrite_task() -> None:
    pass


def test_concurrency_control() -> None:
    """Test concurrency_control configuration"""

    assert test_task.conf.running_concurrency == ConcurrencyControlType.TASK
    assert test_task.conf.registration_concurrency == ConcurrencyControlType.ARGUMENTS
    assert (
        test_overwrite_task.conf.running_concurrency == ConcurrencyControlType.DISABLED
    )


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


def_app_builder = PynencBuilder().task_control().build()
def_app = Pynenc()


@def_app_builder.task
def task_def_app_builder() -> None:
    pass


@def_app.task
def task_def_app() -> None:
    pass


def test_default_values_match() -> None:
    """Test that default values match between PynencBuilder and Pynenc"""

    assert (
        task_def_app_builder.conf.running_concurrency
        == task_def_app.conf.running_concurrency
    )
    assert (
        task_def_app_builder.conf.registration_concurrency
        == task_def_app.conf.registration_concurrency
    )
    assert (
        def_app_builder.broker.conf.queue_timeout_sec
        == def_app.broker.conf.queue_timeout_sec
    )
