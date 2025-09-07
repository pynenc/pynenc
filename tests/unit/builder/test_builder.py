from typing import Any

import pytest

from pynenc import Pynenc
from pynenc.arg_cache import DisabledArgCache, MemArgCache
from pynenc.broker import MemBroker
from pynenc.builder import PynencBuilder
from pynenc.conf.config_broker import ConfigBroker
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.conf.config_runner import (
    ConfigMultiThreadRunner,
    ConfigPersistentProcessRunner,
    ConfigRunner,
    ConfigThreadRunner,
)
from pynenc.orchestrator import MemOrchestrator
from pynenc.runner import (
    DummyRunner,
    MultiThreadRunner,
    PersistentProcessRunner,
    ProcessRunner,
    ThreadRunner,
)
from pynenc.serializer import JsonSerializer, PickleSerializer
from pynenc.state_backend import MemStateBackend
from pynenc.trigger import DisabledTrigger, MemTrigger


def test_basic_builder_should_create_valid_pynenc_app() -> None:
    """Test the most basic builder usage creates a valid Pynenc app."""
    app = PynencBuilder().build()

    assert isinstance(app, Pynenc)
    assert app.app_id is not None  # Should have a default app_id


def test_custom_app_id_should_be_set_correctly() -> None:
    """Test that we can set a custom app_id."""
    app_id = "test.app.id"
    app = PynencBuilder().custom_config(app_id=app_id).build()

    assert app.app_id == app_id


def test_app_id_method_should_set_application_id() -> None:
    """Test setting the application ID directly with app_id method."""
    test_id = "test.application.id"
    app = PynencBuilder().app_id(test_id).build()

    assert app.app_id == test_id
    assert app.conf.app_id == test_id


def test_app_id_method_should_match_custom_config() -> None:
    """Test that app_id() and custom_config(app_id=) produce identical results."""
    test_id = "test.app.id"

    app1 = PynencBuilder().app_id(test_id).build()
    app2 = PynencBuilder().custom_config(app_id=test_id).build()

    assert app1.app_id == app2.app_id
    assert app1.conf.app_id == app2.conf.app_id


def test_memory_components_should_configure_correctly() -> None:
    """Test that memory() correctly configures in-memory components."""
    app = PynencBuilder().memory().build()

    # Check component classes are set correctly
    assert app.conf.orchestrator_cls == "MemOrchestrator"
    assert app.conf.broker_cls == "MemBroker"
    assert app.conf.state_backend_cls == "MemStateBackend"
    assert app.conf.arg_cache_cls == "MemArgCache"

    # Instantiate components and check their types
    assert isinstance(app.orchestrator, MemOrchestrator)
    assert isinstance(app.broker, MemBroker)
    assert isinstance(app.state_backend, MemStateBackend)
    assert isinstance(app.arg_cache, MemArgCache)


def test_mem_arg_cache_should_configure_with_default_values() -> None:
    """Test memory arg cache with default config values."""
    app = PynencBuilder().mem_arg_cache().build()

    assert app.conf.arg_cache_cls == "MemArgCache"
    assert isinstance(app.arg_cache, MemArgCache)
    assert app.arg_cache.conf.min_size_to_cache == 1024  # Default value
    assert app.arg_cache.conf.local_cache_size == 1024  # Default value


def test_mem_arg_cache_should_accept_custom_values() -> None:
    """Test memory arg cache with custom min_size_to_cache and local_cache_size."""
    app = (
        PynencBuilder()
        .mem_arg_cache(min_size_to_cache=512, local_cache_size=2000)
        .build()
    )

    assert app.conf.arg_cache_cls == "MemArgCache"
    assert isinstance(app.arg_cache, MemArgCache)
    assert app.arg_cache.conf.min_size_to_cache == 512  # Custom value
    assert app.arg_cache.conf.local_cache_size == 2000  # Custom value


def test_disable_arg_cache_should_disable_caching() -> None:
    """Test that disable_arg_cache correctly disables argument caching."""
    app = PynencBuilder().disable_arg_cache().build()

    assert app.conf.arg_cache_cls == "DisabledArgCache"
    assert isinstance(app.arg_cache, DisabledArgCache)


def test_mem_trigger_should_configure_with_default_values() -> None:
    """Test memory trigger with default config values."""
    app = PynencBuilder().mem_trigger().build()

    assert app.conf.trigger_cls == "MemTrigger"
    assert isinstance(app.trigger, MemTrigger)
    assert app.trigger.conf.scheduler_interval_seconds == 60  # Default value
    assert app.trigger.conf.enable_scheduler is True  # Default value


def test_mem_trigger_should_accept_custom_values() -> None:
    """Test memory trigger with custom scheduler_interval_seconds and enable_scheduler."""
    app = (
        PynencBuilder()
        .mem_trigger(scheduler_interval_seconds=30, enable_scheduler=False)
        .build()
    )

    assert app.conf.trigger_cls == "MemTrigger"
    assert isinstance(app.trigger, MemTrigger)
    assert app.trigger.conf.scheduler_interval_seconds == 30  # Custom value
    assert app.trigger.conf.enable_scheduler is False  # Custom value


def test_disable_trigger_should_disable_triggers() -> None:
    """Test that disable_trigger correctly disables trigger functionality."""
    app = PynencBuilder().disable_trigger().build()

    assert app.conf.trigger_cls == "DisabledTrigger"
    assert isinstance(app.trigger, DisabledTrigger)


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


def test_memory_compatibility_validation_should_work_with_compatible_runners() -> None:
    """Test validation for memory components with compatible runners."""
    # These should work fine
    PynencBuilder().memory().dummy_runner().build()
    PynencBuilder().memory().thread_runner().build()


def test_memory_compatibility_validation_should_fail_with_incompatible_runners() -> (
    None
):
    """Test validation for memory components with incompatible runners."""
    # These should raise errors
    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().multi_thread_runner().build()

    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().persistent_process_runner().build()

    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().process_runner().build()


def test_dev_mode_should_configure_correctly() -> None:
    """Test dev_mode configuration."""
    app = PynencBuilder().dev_mode(force_sync_tasks=True).build()

    assert app.conf.dev_mode_force_sync_tasks is True


def test_logging_level_should_accept_valid_levels() -> None:
    """Test logging_level configuration and validation."""
    # Valid logging levels
    for level in ["debug", "info", "warning", "error", "critical"]:
        app = PynencBuilder().logging_level(level).build()
        assert app.conf.logging_level == level


def test_logging_level_should_reject_invalid_levels() -> None:
    """Test logging_level validation with invalid input."""
    with pytest.raises(ValueError, match="Invalid logging level"):
        PynencBuilder().logging_level("invalid_level").build()


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


def test_task_control_should_configure_correctly() -> None:
    """Test task_control configuration."""
    app = (
        PynencBuilder()
        .task_control(
            cycle_control=True,
            blocking_control=True,
            queue_timeout_sec=0.05,
        )
        .build()
    )

    assert isinstance(app.orchestrator.conf, ConfigOrchestrator)
    assert app.orchestrator.conf.cycle_control is True
    assert app.orchestrator.conf.blocking_control is True
    assert isinstance(app.broker.conf, ConfigBroker)
    assert app.broker.conf.queue_timeout_sec == 0.05


def test_serializer_should_accept_shortnames() -> None:
    """Test serializer configuration with shortnames."""
    # Test shortnames
    app_json = PynencBuilder().serializer("json").build()
    assert app_json.conf.serializer_cls == "JsonSerializer"
    assert isinstance(app_json.serializer, JsonSerializer)

    app_pickle = PynencBuilder().serializer("pickle").build()
    assert app_pickle.conf.serializer_cls == "PickleSerializer"
    assert isinstance(app_pickle.serializer, PickleSerializer)


def test_serializer_should_accept_class_names() -> None:
    """Test serializer configuration with class names."""
    # Test full class names
    app_json = PynencBuilder().serializer("JsonSerializer").build()
    assert app_json.conf.serializer_cls == "JsonSerializer"

    app_pickle = PynencBuilder().serializer("PickleSerializer").build()
    assert app_pickle.conf.serializer_cls == "PickleSerializer"


def test_serializer_should_reject_invalid_names() -> None:
    """Test serializer validation."""
    with pytest.raises(ValueError, match="Invalid serializer"):
        PynencBuilder().serializer("invalid_serializer").build()


def test_max_pending_seconds_should_configure_correctly() -> None:
    """Test max_pending_seconds configuration."""
    app = PynencBuilder().max_pending_seconds(300).build()

    assert app.conf.max_pending_seconds == 300


def test_hide_arguments_should_configure_correctly() -> None:
    """Test hide_arguments configuration."""
    app = PynencBuilder().hide_arguments().build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN
    # truncate_arguments_length is not set by this method, so it should be the default (32)
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_argument_keys_should_configure_correctly() -> None:
    """Test show_argument_keys configuration."""
    app = PynencBuilder().show_argument_keys().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.KEYS
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_full_arguments_should_configure_correctly() -> None:
    """Test show_full_arguments configuration."""
    app = PynencBuilder().show_full_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.FULL
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_truncated_arguments_should_use_default_length() -> None:
    """Test show_truncated_arguments with default truncate_length."""
    app = PynencBuilder().show_truncated_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 32  # Default value


def test_show_truncated_arguments_should_accept_custom_length() -> None:
    """Test show_truncated_arguments with a custom truncate_length."""
    app = PynencBuilder().show_truncated_arguments(truncate_length=100).build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 100  # Custom value


def test_argument_print_mode_should_accept_string_mode() -> None:
    """Test argument_print_mode with string input."""
    app = PynencBuilder().argument_print_mode(mode="HIDDEN").build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN


def test_argument_print_mode_should_match_default_truncate_length() -> None:
    """Test that the default truncate_length matches the config default."""
    app_builder = PynencBuilder().argument_print_mode(mode="HIDDEN").build()
    app = Pynenc()
    assert (
        app_builder.conf.truncate_arguments_length == app.conf.truncate_arguments_length
    )


def test_show_truncated_arguments_should_reject_invalid_length() -> None:
    """Test show_truncated_arguments with an invalid (non-positive) truncate_length."""
    builder = PynencBuilder()
    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=0).build()

    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=-1).build()


def test_custom_config_should_set_arbitrary_values() -> None:
    """Test custom_config for arbitrary config values."""
    app = (
        PynencBuilder()
        .custom_config(
            app_id="app0",
            dev_mode_force_sync_tasks=True,
            max_pending_seconds=2.5,
        )
        .build()
    )

    assert app.conf.app_id == "app0"
    assert app.conf.dev_mode_force_sync_tasks
    assert app.conf.max_pending_seconds == 2.5


def test_method_chaining_should_work_correctly() -> None:
    """Test complex method chaining with multiple configurations."""
    app = (
        PynencBuilder()
        .memory()
        .serializer("pickle")
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
        .serializer("pickle")
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


def test_unknown_method_should_raise_helpful_error() -> None:
    """Test that unknown methods raise helpful error messages."""
    builder = PynencBuilder()

    with pytest.raises(AttributeError, match="This method may be provided by a plugin"):
        builder.unknown_method()


def test_plugin_method_registration_should_work() -> None:
    """Test that plugin methods can be registered and called."""

    # Mock plugin method that sets a standard config value
    def mock_plugin_method(builder: "PynencBuilder", value: str) -> "PynencBuilder":
        # Use a standard config field instead of custom one
        builder._config["app_id"] = value
        return builder

    # Register the method
    PynencBuilder.register_plugin_method("mock_plugin", mock_plugin_method)

    try:
        # Use the registered method
        app = PynencBuilder().mock_plugin("test_value").build()
        assert app.conf.app_id == "test_value"
    finally:
        # Clean up
        if "mock_plugin" in PynencBuilder._plugin_methods:
            del PynencBuilder._plugin_methods["mock_plugin"]


def test_plugin_validator_registration_should_work() -> None:
    """Test that plugin validators can be registered and are called during build."""

    # Mock validator that requires app_id to be set
    def mock_validator(config: dict[str, Any]) -> None:
        if not config.get("app_id"):
            raise ValueError("app_id is required by validator")

    # Register the validator
    PynencBuilder.register_plugin_validator(mock_validator)

    try:
        # Should fail validation (default app_id might be empty or None)
        with pytest.raises(ValueError, match="app_id is required by validator"):
            PynencBuilder().custom_config(app_id="").build()

        # Should pass validation
        app = PynencBuilder().custom_config(app_id="valid_app_id").build()
        assert app.conf.app_id == "valid_app_id"
    finally:
        # Clean up
        if mock_validator in PynencBuilder._plugin_validators:
            PynencBuilder._plugin_validators.remove(mock_validator)


def test_trigger_configuration_with_memory_components() -> None:
    """Test trigger configuration when using memory components."""
    # Test basic memory trigger setup
    app = PynencBuilder().mem_trigger().build()

    assert app.conf.trigger_cls == "MemTrigger"
    assert isinstance(app.trigger, MemTrigger)


def test_trigger_configuration_with_disabled_trigger() -> None:
    """Test disabled trigger configuration."""
    app = PynencBuilder().disable_trigger().build()

    assert app.conf.trigger_cls == "DisabledTrigger"
    assert isinstance(app.trigger, DisabledTrigger)


def test_redis_trigger_import() -> None:
    """Test Redis trigger functionality."""
    pytest.skip("RedisTrigger not available in current implementation")
