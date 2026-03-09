from typing import Any

import pytest

from pynenc.builder import PynencBuilder


def test_plugin_method_registration_should_work() -> None:
    """Test that plugin methods can be registered and called."""

    # Mock plugin method that sets a standard config value
    def mock_plugin_method(builder: "PynencBuilder", value: str) -> "PynencBuilder":
        # Use a standard config field that actually exists
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

    # Mock validator that requires app_id to be non-empty
    def mock_validator(config: dict[str, Any]) -> None:
        if not config.get("app_id"):
            raise ValueError("app_id is required by validator")

    # Register the validator
    PynencBuilder.register_plugin_validator(mock_validator)

    try:
        # Should fail validation when app_id is empty
        with pytest.raises(ValueError, match="app_id is required by validator"):
            PynencBuilder().custom_config(app_id="").build()

        # Should pass validation when app_id is set
        app = PynencBuilder().custom_config(app_id="valid_app_id").build()
        assert app.conf.app_id == "valid_app_id"
    finally:
        # Clean up
        if mock_validator in PynencBuilder._plugin_validators:
            PynencBuilder._plugin_validators.remove(mock_validator)


def test_unknown_method_should_raise_helpful_error() -> None:
    """Test that unknown methods raise helpful error messages."""
    builder = PynencBuilder()

    with pytest.raises(AttributeError, match="This method may be provided by a plugin"):
        builder.unknown_method()


def test_plugin_method_should_return_builder_for_chaining() -> None:
    """Test that plugin methods return the builder instance for method chaining."""

    def chainable_plugin_method(
        builder: "PynencBuilder", level: str
    ) -> "PynencBuilder":
        builder._config["logging_level"] = level
        return builder

    # Register the method
    PynencBuilder.register_plugin_method("set_log_level", chainable_plugin_method)

    try:
        # Test method chaining works
        app = (
            PynencBuilder()
            .set_log_level("debug")
            .custom_config(app_id="chained_app")
            .build()
        )
        assert app.conf.logging_level == "debug"
        assert app.conf.app_id == "chained_app"
    finally:
        # Clean up
        if "set_log_level" in PynencBuilder._plugin_methods:
            del PynencBuilder._plugin_methods["set_log_level"]


def test_multiple_plugin_validators_should_all_be_called() -> None:
    """Test that multiple registered validators are all executed during build."""

    # First validator checks app_id
    def validator1(config: dict[str, Any]) -> None:
        if not config.get("app_id"):
            raise ValueError("validator1: app_id required")

    # Second validator checks logging_level
    def validator2(config: dict[str, Any]) -> None:
        if config.get("logging_level") == "invalid":
            raise ValueError("validator2: invalid logging level")

    # Register both validators
    PynencBuilder.register_plugin_validator(validator1)
    PynencBuilder.register_plugin_validator(validator2)

    try:
        # Should fail first validator
        with pytest.raises(ValueError, match="validator1: app_id required"):
            PynencBuilder().custom_config(app_id="", logging_level="info").build()

        # Should fail second validator
        with pytest.raises(ValueError, match="validator2: invalid logging level"):
            PynencBuilder().custom_config(
                app_id="test", logging_level="invalid"
            ).build()

        # Should pass both validators
        app = PynencBuilder().custom_config(app_id="test", logging_level="info").build()
        assert app.conf.app_id == "test"
        assert app.conf.logging_level == "info"
    finally:
        # Clean up
        if validator1 in PynencBuilder._plugin_validators:
            PynencBuilder._plugin_validators.remove(validator1)
        if validator2 in PynencBuilder._plugin_validators:
            PynencBuilder._plugin_validators.remove(validator2)
