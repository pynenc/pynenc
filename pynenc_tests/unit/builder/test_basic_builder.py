import pytest

from pynenc import Pynenc
from pynenc.builder import PynencBuilder


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


def test_unknown_method_should_raise_helpful_error() -> None:
    """Test that unknown methods raise helpful error messages."""
    builder = PynencBuilder()

    with pytest.raises(AttributeError, match="This method may be provided by a plugin"):
        builder.unknown_method()
