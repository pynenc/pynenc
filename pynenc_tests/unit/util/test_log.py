import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from pynenc import context
from pynenc.runner.runner_context import RunnerContext
from pynenc.util.log import (
    ColoredFormatter,
    Colors,
    PynencContextFilter,
    create_logger,
    set_logging_context,
    clear_logging_context,
    get_logging_context,
)

if TYPE_CHECKING:
    from _pytest.logging import LogCaptureFixture

# Mock Pynenc app
mock_app = Mock()
mock_app.app_id = "test_app"
mock_app.conf.logging_level = "INFO"
mock_app.conf.truncate_log_ids = True


def test_create_logger_valid_level() -> None:
    logger = create_logger(mock_app)
    assert logger.level == logging.INFO
    assert logger.name == f"pynenc.{mock_app.app_id}"


def test_create_logger_invalid_level() -> None:
    mock_app.conf.logging_level = "INVALID_LEVEL"
    with pytest.raises(ValueError) as exc_info:
        create_logger(mock_app)
    assert "Invalid log level: INVALID_LEVEL" in str(exc_info.value)


def test_set_logging_context() -> None:
    """Test that logging context can be set and retrieved."""
    # Set up runner context in context.py (single source of truth)
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="test_runner_id",
    )
    context.set_runner_context("test_app", runner_ctx)

    # Set logging-specific context
    set_logging_context(task_id="test_task", invocation_id="test_inv")

    log_context = get_logging_context("test_app")
    assert log_context["task_id"] == "test_task"
    assert log_context["invocation_id"] == "test_inv"
    assert log_context["runner_id"] == "test_runner_id"

    clear_logging_context()
    context.clear_runner_context("test_app")


def test_clear_logging_context() -> None:
    """Test that logging context can be cleared."""
    set_logging_context(task_id="test_task", invocation_id="test_inv")
    clear_logging_context()

    log_context = get_logging_context("test_app")
    assert log_context["task_id"] is None
    assert log_context["invocation_id"] is None


def test_pynenc_context_filter() -> None:
    """Test that PynencContextFilter adds context to log records."""
    # Set up runner context in context.py
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="test_runner_id",
    )
    context.set_runner_context("test_app", runner_ctx)
    set_logging_context(task_id="test_task", invocation_id="test_inv")

    # Create mock conf for the filter
    mock_conf = Mock()
    mock_conf.truncate_log_ids = True

    context_filter = PynencContextFilter("test_app", mock_conf)
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    context_filter.filter(record)

    assert getattr(record, "task_id", None) == "test_task"
    assert getattr(record, "invocation_id", None) == "test_inv"
    assert getattr(record, "runner_ctx", None) == runner_ctx
    assert getattr(record, "truncate_log_ids", None) is True

    clear_logging_context()
    context.clear_runner_context("test_app")


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


def test_colored_formatter_with_context() -> None:
    """Test that ColoredFormatter includes context in formatted output."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )

    # Add context to record (simulating what PynencContextFilter does)
    record.task_id = "test_task"
    record.invocation_id = "test_inv"
    record.runner_ctx = None
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include context prefix with task and invocation (7-char truncation)
    assert "[task:test_task inv:test_in]" in formatted
    assert "Test message" in formatted


def test_colored_formatter_with_runner_context() -> None:
    """Test that ColoredFormatter includes runner context when task context is absent."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Runner message",
        args=(),
        exc_info=None,
    )

    # Add runner context (no task/invocation)
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="runner_123456789",  # Will be truncated to 7 chars
    )
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = runner_ctx
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include runner context prefix (7-char truncation)
    assert "[TestRunner(runner_)]" in formatted
    assert "Runner message" in formatted


def test_colored_formatter_with_all_context() -> None:
    """Test that ColoredFormatter shows all context when available."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Full context message",
        args=(),
        exc_info=None,
    )

    # Add all context
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="runner_123456789",
    )
    record.task_id = "test_task"
    record.invocation_id = "test_invocation_id"
    record.runner_ctx = runner_ctx
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include all context in order: runner display, task, invocation (7-char truncation)
    assert "[TestRunner(runner_) task:test_task inv:test_in]" in formatted
    assert "Full context message" in formatted


def test_colored_formatter_with_task_only() -> None:
    """Test that ColoredFormatter shows only task when that's all that's available."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Task only message",
        args=(),
        exc_info=None,
    )

    # Add only task context
    record.task_id = "test_task"
    record.invocation_id = None
    record.runner_ctx = None
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include only task context
    assert "[task:test_task]" in formatted
    assert "Task only message" in formatted


def test_colored_formatter_no_context() -> None:
    """Test that ColoredFormatter works without any context."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="No context message",
        args=(),
        exc_info=None,
    )

    # No context added
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should not include any context prefix
    assert "task:" not in formatted
    assert "inv:" not in formatted
    assert "TestRunner" not in formatted
    assert "No context message" in formatted


def test_logging_with_context_integration(caplog: "LogCaptureFixture") -> None:
    """Test end-to-end logging with context."""
    caplog.set_level(logging.DEBUG)

    # Set up runner context
    runner_ctx = RunnerContext(
        runner_cls="TestRunner",
        runner_id="runner_integration",
    )
    context.set_runner_context("test_integration", runner_ctx)

    # Create mock conf for the filter
    mock_conf = Mock()
    mock_conf.truncate_log_ids = True

    # Create logger with context filter
    logger = logging.getLogger("test_integration")
    context_filter = PynencContextFilter("test_integration", mock_conf)
    logger.addFilter(context_filter)

    # Set logging-specific context
    set_logging_context(task_id="task_123", invocation_id="inv_456")

    # Log message
    logger.info("Integration test message")

    # Verify message was logged
    assert any(
        "Integration test message" in record.message for record in caplog.records
    )

    # Verify context was applied
    log_context = get_logging_context("test_integration")
    assert log_context["task_id"] == "task_123"
    assert log_context["invocation_id"] == "inv_456"
    assert log_context["runner_id"] == "runner_integration"

    clear_logging_context()
    context.clear_runner_context("test_integration")
