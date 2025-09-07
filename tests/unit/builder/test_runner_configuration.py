from pynenc.builder import PynencBuilder
from pynenc.conf.config_runner import (
    ConfigMultiThreadRunner,
    ConfigPersistentProcessRunner,
    ConfigRunner,
    ConfigThreadRunner,
)
from pynenc.runner import (
    DummyRunner,
    MultiThreadRunner,
    PersistentProcessRunner,
    ProcessRunner,
    ThreadRunner,
)


def test_multi_thread_runner_should_configure_correctly() -> None:
    """Test MultiThreadRunner configuration."""
    app = (
        PynencBuilder()
        .multi_thread_runner(min_threads=2, max_threads=4, enforce_max_processes=True)
        .build()
    )

    assert app.conf.runner_cls == "MultiThreadRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, MultiThreadRunner)
    assert isinstance(app.runner.conf, ConfigMultiThreadRunner)
    assert app.runner.conf.min_threads == 2
    assert app.runner.conf.max_threads == 4
    assert app.runner.conf.enforce_max_processes is True


def test_persistent_process_runner_should_configure_correctly() -> None:
    """Test PersistentProcessRunner configuration."""
    app = PynencBuilder().persistent_process_runner(num_processes=4).build()

    assert app.conf.runner_cls == "PersistentProcessRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, PersistentProcessRunner)
    assert isinstance(app.runner.conf, ConfigPersistentProcessRunner)
    assert app.runner.conf.num_processes == 4


def test_thread_runner_should_configure_correctly() -> None:
    """Test ThreadRunner configuration."""
    app = PynencBuilder().thread_runner(min_threads=2, max_threads=4).build()

    assert app.conf.runner_cls == "ThreadRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, ThreadRunner)
    assert isinstance(app.runner.conf, ConfigThreadRunner)
    assert app.runner.conf.min_threads == 2
    assert app.runner.conf.max_threads == 4


def test_process_runner_should_configure_correctly() -> None:
    """Test ProcessRunner configuration."""
    app = PynencBuilder().process_runner().build()

    assert app.conf.runner_cls == "ProcessRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, ProcessRunner)


def test_dummy_runner_should_configure_correctly() -> None:
    """Test DummyRunner configuration."""
    app = PynencBuilder().dummy_runner().build()

    assert app.conf.runner_cls == "DummyRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, DummyRunner)


def test_runner_tuning_should_configure_correctly() -> None:
    """Test runner_tuning configuration."""
    app = (
        PynencBuilder()
        .runner_tuning(
            runner_loop_sleep_time_sec=0.05,
            invocation_wait_results_sleep_time_sec=0.02,
            min_parallel_slots=3,
        )
        .build()
    )
    assert isinstance(app.runner.conf, ConfigRunner)
    assert app.runner.conf.runner_loop_sleep_time_sec == 0.05
    assert app.runner.conf.invocation_wait_results_sleep_time_sec == 0.02
    assert app.runner.conf.min_parallel_slots == 3
