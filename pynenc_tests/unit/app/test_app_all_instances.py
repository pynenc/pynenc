import threading
from typing import TYPE_CHECKING, Any

import pytest

from pynenc import Pynenc
from pynenc.exceptions import RetryError
from pynenc.runner import ThreadRunner
from pynenc_tests.conftest import MockPynenc

if TYPE_CHECKING:
    from _pytest.fixtures import FixtureRequest

mock_all = MockPynenc.with_id("unit.test_mem_app")


@mock_all.task
def sum_task(a: int, b: int) -> int:
    return a + b


@mock_all.task
def raise_exception() -> Any:
    raise ValueError("test")


@mock_all.task
def get_text() -> str:
    return "example"


@mock_all.task
def get_upper() -> str:
    return get_text().result.upper()


@mock_all.task(max_retries=1)
def retry_once() -> int:
    if retry_once.invocation.num_retries == 0:
        raise RetryError()
    return retry_once.invocation.num_retries


@pytest.fixture
def app(request: "FixtureRequest", app_instance: Pynenc) -> Pynenc:
    app = app_instance
    app.runner = ThreadRunner(app)
    app._tasks = mock_all._tasks
    sum_task.app = app
    raise_exception.app = app
    get_text.app = app
    get_upper.app = app
    retry_once.app = app
    app.purge()
    request.addfinalizer(app.purge)
    return app


def test_thread_execution(app: Pynenc) -> None:
    """Test the whole lifecycle of a task execution"""

    def run_in_thread() -> None:
        app.runner.run()

    invocation = sum_task(1, 2)
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert invocation.result == 3
    app.runner.stop_runner_loop()


def test_raise_exception(app: Pynenc) -> None:
    """Test that an exception is raised if the task raises an exception"""

    def run_in_thread() -> None:
        app.runner.run()

    invocation = raise_exception()
    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    with pytest.raises(ValueError):
        _ = invocation.result
    app.runner.stop_runner_loop()


def test_sub_invocation_dependency(app: Pynenc) -> None:
    """Test when an invocation requires the result of another invocation"""

    def run_in_thread() -> None:
        app.runner.run()

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()
    assert get_upper().result == "EXAMPLE"
    app.runner.stop_runner_loop()


def test_run_retry(app: Pynenc) -> None:
    """
    Test that the Task will retry once for synchronous invocation
    """

    def run_in_thread() -> None:
        app.conf.runner_cls = "ThreadRunner"
        app.runner.run()

    # Create invocation before starting runner to check initial state
    invocation = retry_once()
    assert invocation.num_retries == 0

    thread = threading.Thread(target=run_in_thread, daemon=True)
    thread.start()

    # After getting result, verify retry happened
    assert invocation.result == 1
    assert invocation.num_retries == 1


def test_pynenc_getstate_setstate() -> None:
    """
    Test the serialization and deserialization of the Pynenc instance via __getstate__ and __setstate__.
    """
    config = {"app_id": "test_app", "runner_cls": "ThreadRunner"}
    app = Pynenc(config_values=config)

    state = app.__getstate__()
    expected_state = {
        "config_values": {"app_id": "test_app", "runner_cls": "ThreadRunner"},
        "config_filepath": None,
        "reporting": None,
        "tasks": {},
    }
    assert state == expected_state

    # Now test setstate by creating a new app and applying the state
    new_app = Pynenc()
    new_app.__setstate__(state)

    assert new_app.app_id == "test_app"
    assert new_app.config_values == {"app_id": "test_app", "runner_cls": "ThreadRunner"}
    assert new_app._runner_instance is None  # should reset runner instance
    assert new_app._tasks == {}  # should reset tasks
