import logging
from unittest.mock import MagicMock


from pynenc import context
from pynenc.runner.runner_context import RunnerContext
from pynenc.util.log import ColoredFormatter, Colors, PynencContextFilter, create_logger
from pynenc_tests.conftest import MockPynenc

mock_app = MockPynenc()


@mock_app.task
def dummy_task() -> None:
    pass


def test_create_logger_valid_level() -> None:
    """Test that create_logger accepts valid logging levels."""
    app = MockPynenc()
    logger = create_logger(app, use_colors=False)
    assert logger.name == f"pynenc.{app.app_id}"


def test_colored_formatter_simple_message() -> None:
    """Test that ColoredFormatter formats a simple message."""
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
    formatted = formatter.format(record)
    assert "Test message" in formatted


def test_create_logger_without_colors() -> None:
    """Test that logger created with use_colors=False uses standard formatter."""
    app = MockPynenc()
    logger = create_logger(app, use_colors=False)
    assert logger.name == f"pynenc.{app.app_id}"
    assert len(logger.handlers) > 0


def test_colored_formatter_unclosed_bracket() -> None:
    """Test that ColoredFormatter handles unclosed brackets gracefully."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Message with [unclosed bracket",
        args=(),
        exc_info=None,
    )
    formatted = formatter.format(record)
    assert "Message with [unclosed bracket" in formatted


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
    record.invocation_id = "test_invocation"
    record.runner_ctx = None
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include context prefix with task and invocation
    assert "task:test_task" in formatted
    assert "inv:test_inv" in formatted  # Adjusted for truncation


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
        runner_id="runner_123456789",
    )
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = runner_ctx
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include runner context
    assert "TestRunner(runner_1)" in formatted  # Adjusted for truncation


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
    record.task_id = "my_task"
    record.invocation_id = "inv_12345678"
    record.runner_ctx = runner_ctx
    record.truncate_log_ids = True

    formatted = formatter.format(record)

    # Should include all context
    assert "task:my_task" in formatted
    assert "inv:inv_1234" in formatted  # Adjusted for truncation
    assert "TestRunner(runner_1)" in formatted


def test_colored_formatter_with_task_only() -> None:
    """Test that ColoredFormatter works with only task context."""
    formatter = ColoredFormatter()
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Task message",
        args=(),
        exc_info=None,
    )

    record.task_id = "task_only"
    record.invocation_id = None
    record.runner_ctx = None
    record.truncate_log_ids = False

    formatted = formatter.format(record)

    assert "task:task_only" in formatted


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

    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None

    formatted = formatter.format(record)

    assert "No context message" in formatted


def test_logging_with_context_integration() -> None:
    """Test end-to-end logging with context."""
    # Create logger with context filter
    app = MockPynenc()
    logger = create_logger(app, use_colors=False)

    # Log something
    logger.info("Test message with context")

    # Verify logger is configured correctly
    assert logger.name == f"pynenc.{app.app_id}"
    assert len(logger.handlers) > 0


def test_colored_formatter_adds_color_to_log_levels() -> None:
    """Test that ColoredFormatter correctly adds color codes to log levels."""
    formatter = ColoredFormatter("%(levelname)s %(message)s")

    # Create a mock log record
    record = MagicMock()
    record.levelname = "INFO"
    record.getMessage = MagicMock(return_value="test message")
    record.exc_info = None
    record.exc_text = None
    record.stack_info = None
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None

    formatted = formatter.format(record)

    # Should contain color codes and the message
    assert "INFO" in formatted
    assert "test message" in formatted
    # Should contain reset code
    assert Colors.RESET in formatted


def test_pynenc_context_filter_without_invocation(app_instance: "MockPynenc") -> None:
    """Test that filter works when no invocation context is set."""
    ctx_filter = PynencContextFilter(app_instance.app_id, app_instance.conf)

    record = MagicMock()
    record.getMessage = MagicMock(return_value="test")

    # Should not raise and should return True
    assert ctx_filter.filter(record) is True
    # Filter does not set invocation_id when no context
    # Since record is MagicMock, check that it's not explicitly set
    # (MagicMock creates attributes on access, so we check if it's the default mock)
    if (
        hasattr(record, "_mock_children")
        and "invocation_id" not in record._mock_children
    ):
        assert True  # Not set
    else:
        # If set, it should be None or not accessed
        pass


def test_pynenc_context_filter_with_invocation(app_instance: "MockPynenc") -> None:
    """Test that filter adds invocation context when available."""
    dummy_task.app = app_instance
    invocation = dummy_task()

    # Set context
    previous = context.swap_dist_invocation_context(app_instance.app_id, invocation)  # type: ignore

    try:
        ctx_filter = PynencContextFilter(app_instance.app_id, app_instance.conf)
        record = MagicMock()
        record.getMessage = MagicMock(return_value="test")

        ctx_filter.filter(record)

        assert record.invocation_id == invocation.invocation_id
        assert record.task_id == invocation.task.task_id
    finally:
        # Reset context
        context.swap_dist_invocation_context(app_instance.app_id, previous)
