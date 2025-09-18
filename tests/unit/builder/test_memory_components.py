import pytest

from pynenc.arg_cache import DisabledArgCache, MemArgCache
from pynenc.broker import MemBroker
from pynenc.builder import PynencBuilder
from pynenc.orchestrator import MemOrchestrator
from pynenc.state_backend import MemStateBackend
from pynenc.trigger import DisabledTrigger, MemTrigger


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
