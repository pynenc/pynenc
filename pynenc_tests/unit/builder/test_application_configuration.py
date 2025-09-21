import pytest

from pynenc.builder import PynencBuilder
from pynenc.conf.config_broker import ConfigBroker
from pynenc.conf.config_orchestrator import ConfigOrchestrator
from pynenc.serializer import JsonPickleSerializer, JsonSerializer, PickleSerializer


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


def test_serializer() -> None:
    """Test serializer configuration with shortnames."""
    # Test shortnames
    app_json = PynencBuilder().serializer_json().build()
    assert isinstance(app_json.serializer, JsonSerializer)

    app_pickle = PynencBuilder().serializer_pickle().build()
    assert isinstance(app_pickle.serializer, PickleSerializer)

    app_json_pickle = PynencBuilder().serializer_json_pickle().build()
    assert isinstance(app_json_pickle.serializer, JsonPickleSerializer)


def test_max_pending_seconds_should_configure_correctly() -> None:
    """Test max_pending_seconds configuration."""
    app = PynencBuilder().max_pending_seconds(300).build()

    assert app.conf.max_pending_seconds == 300
