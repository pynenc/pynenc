import pytest

from pynenc import Pynenc
from pynenc.arg_cache import DisabledArgCache, MemArgCache, RedisArgCache
from pynenc.broker import MemBroker, RedisBroker
from pynenc.builder import PynencBuilder
from pynenc.conf.config_broker import ConfigBroker
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.conf.config_pynenc import ArgumentPrintMode
from pynenc.conf.config_redis import ConfigRedis
from pynenc.conf.config_runner import (
    ConfigMultiThreadRunner,
    ConfigPersistentProcessRunner,
    ConfigRunner,
    ConfigThreadRunner,
)
from pynenc.orchestrator import MemOrchestrator, RedisOrchestrator

# Import components for instance checks
from pynenc.runner import (
    DummyRunner,
    MultiThreadRunner,
    PersistentProcessRunner,
    ProcessRunner,
    ThreadRunner,
)
from pynenc.serializer import JsonSerializer, PickleSerializer
from pynenc.state_backend import MemStateBackend, RedisStateBackend


def test_basic_builder() -> None:
    """Test the most basic builder usage creates a valid Pynenc app"""
    app = PynencBuilder().build()

    assert isinstance(app, Pynenc)
    assert app.app_id is not None  # Should have a default app_id


def test_custom_app_id() -> None:
    """Test that we can set a custom app_id"""
    app_id = "test.app.id"
    app = PynencBuilder().custom_config(app_id=app_id).build()

    assert app.app_id == app_id


def test_app_id() -> None:
    """Test setting the application ID directly with app_id method."""
    test_id = "test.application.id"
    app = PynencBuilder().app_id(test_id).build()

    assert app.app_id == test_id
    assert app.conf.app_id == test_id


def test_app_id_method_vs_custom_config() -> None:
    """Test that app_id() and custom_config(app_id=) produce identical results."""
    test_id = "test.app.id"

    app1 = PynencBuilder().app_id(test_id).build()
    app2 = PynencBuilder().custom_config(app_id=test_id).build()

    assert app1.app_id == app2.app_id
    assert app1.conf.app_id == app2.conf.app_id


def test_redis_components() -> None:
    """Test that redis() correctly configures Redis components"""
    redis_url = "redis://localhost:6379"
    app = PynencBuilder().redis(url=redis_url).build()

    # Check component classes are set correctly
    assert app.conf.orchestrator_cls == "RedisOrchestrator"
    assert app.conf.broker_cls == "RedisBroker"
    assert app.conf.state_backend_cls == "RedisStateBackend"
    assert app.conf.arg_cache_cls == "RedisArgCache"

    # Instantiate components and check their types
    assert isinstance(app.orchestrator, RedisOrchestrator)
    assert isinstance(app.broker, RedisBroker)
    assert isinstance(app.state_backend, RedisStateBackend)
    assert isinstance(app.arg_cache, RedisArgCache)

    # Check Redis URL is set correctly
    assert app.broker.conf.redis_url == redis_url


def test_redis_with_db() -> None:
    """Test redis() with db parameter"""
    redis_url = "redis://localhost:6379"
    db = 5
    app = PynencBuilder().redis(url=redis_url, db=db).build()

    assert isinstance(app.broker.conf, ConfigRedis)
    assert app.broker.conf.redis_url == f"{redis_url}/{db}"


def test_memory_components() -> None:
    """Test that memory() correctly configures in-memory components"""
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


def test_arg_cache_modes() -> None:
    """Test different arg_cache modes with default config values"""
    # Redis arg cache
    app_redis = (
        PynencBuilder()
        .redis(url="redis://localhost:6379")
        .arg_cache(mode="redis")
        .build()
    )
    assert app_redis.conf.arg_cache_cls == "RedisArgCache"
    assert isinstance(app_redis.arg_cache, RedisArgCache)
    assert app_redis.arg_cache.conf.min_size_to_cache == 1024  # Default value
    assert app_redis.arg_cache.conf.local_cache_size == 1024  # Default value

    # Memory arg cache
    app_memory = PynencBuilder().arg_cache(mode="memory").build()
    assert app_memory.conf.arg_cache_cls == "MemArgCache"
    assert isinstance(app_memory.arg_cache, MemArgCache)
    assert app_memory.arg_cache.conf.min_size_to_cache == 1024  # Default value
    assert app_memory.arg_cache.conf.local_cache_size == 1024  # Default value

    # Disabled arg cache
    app_disabled = PynencBuilder().arg_cache(mode="disabled").build()
    assert app_disabled.conf.arg_cache_cls == "DisabledArgCache"
    assert isinstance(app_disabled.arg_cache, DisabledArgCache)
    assert app_disabled.arg_cache.conf.min_size_to_cache == 1024  # Default value
    assert app_disabled.arg_cache.conf.local_cache_size == 1024  # Default value


def test_arg_cache_custom_config() -> None:
    """Test arg_cache with custom min_size_to_cache and local_cache_size"""
    # Redis with custom values
    app_redis_custom = (
        PynencBuilder()
        .redis(url="redis://localhost:6379")
        .arg_cache(mode="redis", min_size_to_cache=2048, local_cache_size=500)
        .build()
    )
    assert app_redis_custom.conf.arg_cache_cls == "RedisArgCache"
    assert isinstance(app_redis_custom.arg_cache, RedisArgCache)
    assert app_redis_custom.arg_cache.conf.min_size_to_cache == 2048  # Custom value
    assert app_redis_custom.arg_cache.conf.local_cache_size == 500  # Custom value

    # Memory with custom values
    app_memory_custom = (
        PynencBuilder()
        .arg_cache(mode="memory", min_size_to_cache=512, local_cache_size=2000)
        .build()
    )
    assert app_memory_custom.conf.arg_cache_cls == "MemArgCache"
    assert isinstance(app_memory_custom.arg_cache, MemArgCache)
    assert app_memory_custom.arg_cache.conf.min_size_to_cache == 512  # Custom value
    assert app_memory_custom.arg_cache.conf.local_cache_size == 2000  # Custom value

    # Disabled with custom values (still applied, though caching is off)
    app_disabled_custom = (
        PynencBuilder()
        .arg_cache(mode="disabled", min_size_to_cache=100, local_cache_size=10)
        .build()
    )
    assert app_disabled_custom.conf.arg_cache_cls == "DisabledArgCache"
    assert isinstance(app_disabled_custom.arg_cache, DisabledArgCache)
    assert app_disabled_custom.arg_cache.conf.min_size_to_cache == 100  # Custom value
    assert app_disabled_custom.arg_cache.conf.local_cache_size == 10  # Custom value


def test_arg_cache_match_default_config() -> None:
    """Test that the default arg_cache config values match the app's config"""
    app_builder = PynencBuilder().arg_cache(mode="memory").build()
    app = Pynenc()
    assert (
        app.arg_cache.conf.min_size_to_cache
        == app_builder.arg_cache.conf.min_size_to_cache
    )
    assert (
        app.arg_cache.conf.local_cache_size
        == app_builder.arg_cache.conf.local_cache_size
    )


def test_redis_arg_cache_validation() -> None:
    """Test validation for Redis arg cache without Redis configuration"""
    builder = PynencBuilder()
    with pytest.raises(
        ValueError, match="Redis arg cache requires redis configuration"
    ):
        builder.arg_cache(mode="redis").build()


def test_multi_thread_runner() -> None:
    """Test MultiThreadRunner configuration"""
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


def test_persistent_process_runner() -> None:
    """Test PersistentProcessRunner configuration"""
    app = PynencBuilder().persistent_process_runner(num_processes=4).build()

    assert app.conf.runner_cls == "PersistentProcessRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, PersistentProcessRunner)
    assert isinstance(app.runner.conf, ConfigPersistentProcessRunner)
    assert app.runner.conf.num_processes == 4


def test_thread_runner() -> None:
    """Test ThreadRunner configuration"""
    app = PynencBuilder().thread_runner(min_threads=2, max_threads=4).build()

    assert app.conf.runner_cls == "ThreadRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, ThreadRunner)
    assert isinstance(app.runner.conf, ConfigThreadRunner)
    assert app.runner.conf.min_threads == 2
    assert app.runner.conf.max_threads == 4


def test_process_runner() -> None:
    """Test ProcessRunner configuration"""
    app = PynencBuilder().process_runner().build()

    assert app.conf.runner_cls == "ProcessRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, ProcessRunner)


def test_dummy_runner() -> None:
    """Test DummyRunner configuration"""
    app = PynencBuilder().dummy_runner().build()

    assert app.conf.runner_cls == "DummyRunner"

    # Initialize runner and check its type
    assert isinstance(app.runner, DummyRunner)


def test_memory_compatibility_validation() -> None:
    """Test validation for memory components with incompatible runners"""
    # These should work fine
    PynencBuilder().memory().dummy_runner().build()
    PynencBuilder().memory().thread_runner().build()

    # These should raise errors
    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().multi_thread_runner().build()

    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().persistent_process_runner().build()

    with pytest.raises(ValueError, match="not compatible with in-memory components"):
        PynencBuilder().memory().process_runner().build()


def test_dev_mode() -> None:
    """Test dev_mode configuration"""
    app = PynencBuilder().dev_mode(force_sync_tasks=True).build()

    assert app.conf.dev_mode_force_sync_tasks is True


def test_logging_level() -> None:
    """Test logging_level configuration and validation"""
    # Valid logging levels
    for level in ["debug", "info", "warning", "error", "critical"]:
        app = PynencBuilder().logging_level(level).build()
        assert app.conf.logging_level == level

    # Invalid logging level
    with pytest.raises(ValueError, match="Invalid logging level"):
        PynencBuilder().logging_level("invalid_level").build()


def test_runner_tuning() -> None:
    """Test runner_tuning configuration"""
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


def test_task_control() -> None:
    """Test task_control configuration"""
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


def test_serializer_shortnames() -> None:
    """Test serializer configuration with shortnames"""
    # Test shortnames
    app_json = PynencBuilder().serializer("json").build()
    assert app_json.conf.serializer_cls == "JsonSerializer"
    assert isinstance(app_json.serializer, JsonSerializer)

    app_pickle = PynencBuilder().serializer("pickle").build()
    assert app_pickle.conf.serializer_cls == "PickleSerializer"
    assert isinstance(app_pickle.serializer, PickleSerializer)


def test_serializer_classnames() -> None:
    """Test serializer configuration with class names"""
    # Test full class names
    app_json = PynencBuilder().serializer("JsonSerializer").build()
    assert app_json.conf.serializer_cls == "JsonSerializer"

    app_pickle = PynencBuilder().serializer("PickleSerializer").build()
    assert app_pickle.conf.serializer_cls == "PickleSerializer"


def test_serializer_validation() -> None:
    """Test serializer validation"""
    with pytest.raises(ValueError, match="Invalid serializer"):
        PynencBuilder().serializer("invalid_serializer").build()


def test_max_pending_seconds() -> None:
    """Test max_pending_seconds configuration"""
    app = PynencBuilder().max_pending_seconds(300).build()

    assert app.conf.max_pending_seconds == 300


def test_hide_arguments() -> None:
    """Test hide_arguments configuration"""
    app = PynencBuilder().hide_arguments().build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN
    # truncate_arguments_length is not set by this method, so it should be the default (32)
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_argument_keys() -> None:
    """Test show_argument_keys configuration"""
    app = PynencBuilder().show_argument_keys().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.KEYS
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_full_arguments() -> None:
    """Test show_full_arguments configuration"""
    app = PynencBuilder().show_full_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.FULL
    # truncate_arguments_length is not set by this method, so it should be the default
    assert app.conf.truncate_arguments_length == 32  # Default from ConfigPynenc


def test_show_truncated_arguments_default() -> None:
    """Test show_truncated_arguments with default truncate_length"""
    app = PynencBuilder().show_truncated_arguments().build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 32  # Default value


def test_show_truncated_arguments_custom_length() -> None:
    """Test show_truncated_arguments with a custom truncate_length"""
    app = PynencBuilder().show_truncated_arguments(truncate_length=100).build()

    assert app.conf.print_arguments is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 100  # Custom value


def test_argument_print_mode_string() -> None:
    """Test show_truncated_arguments with a custom truncate_length"""
    app = PynencBuilder().argument_print_mode(mode="HIDDEN").build()

    assert app.conf.print_arguments is False
    assert app.conf.argument_print_mode == ArgumentPrintMode.HIDDEN


def test_argument_print_mode_defaults() -> None:
    """Test that the default truncate_length match the cofig default"""
    app_builder = PynencBuilder().argument_print_mode(mode="HIDDEN").build()
    app = Pynenc()
    assert (
        app_builder.conf.truncate_arguments_length == app.conf.truncate_arguments_length
    )


def test_show_truncated_arguments_invalid_length() -> None:
    """Test show_truncated_arguments with an invalid (non-positive) truncate_length"""
    builder = PynencBuilder()
    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=0).build()

    with pytest.raises(ValueError, match="truncate_length must be greater than 0"):
        builder.show_truncated_arguments(truncate_length=-1).build()


def test_custom_config() -> None:
    """Test custom_config for arbitrary config values"""
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


def test_method_chaining() -> None:
    """Test complex method chaining with multiple configurations"""
    app = (
        PynencBuilder()
        .redis(url="redis://localhost:6379", db=1)
        .serializer("pickle")
        .multi_thread_runner(min_threads=2, max_threads=8)
        .logging_level("info")
        .runner_tuning(runner_loop_sleep_time_sec=0.01)
        .task_control(cycle_control=True)
        .show_argument_keys()
        .max_pending_seconds(60)
        .build()
    )

    # Check a sampling of the configurations
    assert app.broker.conf.redis_url == "redis://localhost:6379/1"
    assert app.conf.serializer_cls == "PickleSerializer"
    assert app.conf.runner_cls == "MultiThreadRunner"
    assert app.runner.conf.min_threads == 2
    assert app.runner.conf.max_threads == 8
    assert app.conf.logging_level == "info"
    assert app.runner.conf.runner_loop_sleep_time_sec == 0.01
    assert app.orchestrator.conf.cycle_control is True
    assert app.conf.argument_print_mode == ArgumentPrintMode.KEYS
    assert app.conf.max_pending_seconds == 60


def test_complete_app_example() -> None:
    """A complete example of building and configuring a Pynenc app"""
    # This test demonstrates how the builder would be used in real code
    app = (
        PynencBuilder()
        .redis(url="redis://localhost:6379", db=14)
        .serializer("pickle")
        .multi_thread_runner(min_threads=1, max_threads=4)
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
    assert isinstance(app.runner, MultiThreadRunner)
    assert isinstance(app.broker, RedisBroker)
    assert isinstance(app.orchestrator, RedisOrchestrator)
    assert isinstance(app.state_backend, RedisStateBackend)
    assert isinstance(app.arg_cache, RedisArgCache)
    assert isinstance(app.serializer, PickleSerializer)
    assert app.conf.argument_print_mode == ArgumentPrintMode.TRUNCATED
    assert app.conf.truncate_arguments_length == 33
    assert app.conf.max_pending_seconds == 120
    assert app.conf.logging_level == "info"
    assert app.runner.conf.runner_loop_sleep_time_sec == 0.01
    assert app.orchestrator.conf.cycle_control is True
    assert app.broker.conf.redis_url == "redis://localhost:6379/14"
