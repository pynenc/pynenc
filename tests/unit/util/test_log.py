import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from pynenc.util.log import (
    ColoredFormatter,
    Colors,
    RunnerLogAdapter,
    TaskLoggerAdapter,
    create_logger,
)

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

# Mock Pynenc app
mock_app = Mock()
mock_app.app_id = "test_app"
mock_app.conf.logging_level = "INFO"


def test_create_logger_valid_level() -> None:
    logger = create_logger(mock_app)
    assert logger.level == logging.INFO
    assert logger.name == f"pynenc.{mock_app.app_id}"


def test_create_logger_invalid_level() -> None:
    mock_app.conf.logging_level = "INVALID_LEVEL"
    with pytest.raises(ValueError) as exc_info:
        create_logger(mock_app)
    assert "Invalid log level: INVALID_LEVEL" in str(exc_info.value)


def test_task_logger_adapter(caplog: "LogCaptureFixture") -> None:
    caplog.set_level(logging.DEBUG)
    base_logger = logging.getLogger("test")
    task_logger = TaskLoggerAdapter(base_logger, "task_id", "invocation_id")

    test_message = "Test message"
    expected_log_message = f"[task_id: invocation_id] {test_message}"

    task_logger.info(test_message)

    # Check if the expected log message is in the caplog record
    assert any(expected_log_message in record.message for record in caplog.records)


def test_task_logger_adapter_without_invocation_id(caplog: "LogCaptureFixture") -> None:
    caplog.set_level(logging.DEBUG)
    base_logger = logging.getLogger("test")
    task_logger = TaskLoggerAdapter(base_logger, "task_id")

    test_message = "Test message"
    expected_log_message = f"[task_id] {test_message}"

    task_logger.info(test_message)

    # Check if the expected log message is in the caplog record
    assert any(expected_log_message in record.message for record in caplog.records)


def test_runner_log_adapter(caplog: "LogCaptureFixture") -> None:
    caplog.set_level(logging.DEBUG)
    base_logger = logging.getLogger("test")
    runner_logger = RunnerLogAdapter(base_logger, "runner_id")

    test_message = "Runner message"
    expected_log_message = f"[runner: runner_id] {test_message}"

    runner_logger.info(test_message)

    # Check if the expected log message is in the caplog record
    assert any(expected_log_message in record.message for record in caplog.records)


def test_colored_formatter_simple_message() -> None:
    """Test that non-bracketed messages are colored correctly."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Simple message",  # Note: not starting with [
        args=(),
        exc_info=None,
    )

    formatted = formatter.format(record)
    expected_color = Colors.GREEN  # INFO level color

    # Check that the message is wrapped with color codes
    assert f"{expected_color}Simple message{Colors.RESET}" in formatted


def test_create_logger_without_colors() -> None:
    """Test that logger created with use_colors=False uses standard formatter."""
    mock_app = Mock()
    mock_app.app_id = "test_app"
    mock_app.conf.logging_level = "INFO"

    # Create logger with colors disabled
    logger = create_logger(mock_app, use_colors=False)

    # Get the formatter from the logger's handler
    formatter = logger.handlers[0].formatter

    # Verify it's a standard Formatter, not ColoredFormatter
    assert isinstance(formatter, logging.Formatter)
    assert not isinstance(formatter, ColoredFormatter)

    # Test the formatting
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.created = 1709459430.123  # Example timestamp
    record.msecs = 123

    formatted = formatter.format(record)

    # Verify format without color codes
    assert "INFO     test_logger Test message" in formatted
    assert "\033[" not in formatted  # No ANSI color codes


def test_colored_formatter_unclosed_bracket() -> None:
    """Test coloring of message that starts with [ but has no closing bracket."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="[unclosed bracket message",  # Note: starts with [ but no closing ]
        args=(),
        exc_info=None,
    )

    formatted = formatter.format(record)
    expected_color = Colors.GREEN  # INFO level color

    # Check that the entire message is wrapped with color codes
    assert f"{expected_color}[unclosed bracket message{Colors.RESET}" in formatted
    # Verify there's no split coloring (which would happen if treated as prefix)
    assert f"{Colors.RESET}{expected_color}" not in formatted
