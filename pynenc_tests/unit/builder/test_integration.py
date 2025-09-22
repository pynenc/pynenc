from pynenc import Pynenc
from pynenc.arg_cache import MemArgCache
from pynenc.broker import MemBroker
from pynenc.builder import PynencBuilder
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.orchestrator import MemOrchestrator
from pynenc.runner import ThreadRunner
from pynenc.serializer import PickleSerializer
from pynenc.state_backend import MemStateBackend


def test_method_chaining_should_work_correctly() -> None:
    """Test complex method chaining with multiple configurations."""
    app = (
        PynencBuilder()
        .memory()
        .serializer_pickle()
        .thread_runner(min_threads=2, max_threads=8)
        .logging_level("info")
        .runner_tuning(runner_loop_sleep_time_sec=0.01)
        .task_control(cycle_control=True)
        .show_argument_keys()
        .max_pending_seconds(60)
        .build()
    )

    # Check a sampling of the configurations
    assert app.conf.orchestrator_cls == "MemOrchestrator"
    assert app.conf.serializer_cls == "PickleSerializer"
    assert app.conf.runner_cls == "ThreadRunner"
    assert app.runner.conf.min_threads == 2
    assert app.runner.conf.max_threads == 8
    assert app.conf.logging_level == "info"
    assert app.runner.conf.runner_loop_sleep_time_sec == 0.01
    assert app.orchestrator.conf.cycle_control is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.KEYS
    assert app.conf.max_pending_seconds == 60


def test_complete_app_example_should_work() -> None:
    """A complete example of building and configuring a Pynenc app."""
    # This test demonstrates how the builder would be used in real code
    app = (
        PynencBuilder()
        .memory()
        .serializer_pickle()
        .thread_runner(min_threads=1, max_threads=4)
        .logging_level("info")
        .runner_tuning(
            runner_loop_sleep_time_sec=0.01,
            invocation_wait_results_sleep_time_sec=0.01,
        )
        .task_control(cycle_control=True)
        .show_truncated_arguments(truncate_length=33)
        .max_pending_seconds(120)
        .custom_config(app_id="example.app")
        .build()
    )

    # Verify the app is correctly configured
    assert isinstance(app, Pynenc)
    assert app.app_id == "example.app"
    assert isinstance(app.runner, ThreadRunner)
    assert isinstance(app.broker, MemBroker)
    assert isinstance(app.orchestrator, MemOrchestrator)
    assert isinstance(app.state_backend, MemStateBackend)
    assert isinstance(app.arg_cache, MemArgCache)
    assert isinstance(app.serializer, PickleSerializer)
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 33
    assert app.conf.max_pending_seconds == 120
    assert app.conf.logging_level == "info"
    assert app.runner.conf.runner_loop_sleep_time_sec == 0.01
    assert app.orchestrator.conf.cycle_control is True
