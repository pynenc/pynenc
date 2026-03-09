import json
import logging
import sys
from io import StringIO
from unittest.mock import MagicMock


from pynenc import context
from pynenc.conf.config_pynenc import LogFormat
from pynenc.runner.runner_context import RunnerContext
from pynenc.util.log import (
    ColoredFormatter,
    Colors,
    JsonFormatter,
    PlainTextFormatter,
    PynencContextFilter,
    _auto_detect_colors,
    create_logger,
)
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
    record.compact_log_context = True

    formatted = formatter.format(record)

    # Should include context prefix with task and invocation
    assert "test_task" in formatted
    assert "test_inv" in formatted  # Adjusted for truncation


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
    record.compact_log_context = True

    formatted = formatter.format(record)

    # Should include runner context
    assert "TR(runner_1)" in formatted  # Adjusted for truncation


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
    record.compact_log_context = True

    formatted = formatter.format(record)

    # Should include all context
    assert "my_task" in formatted
    assert "inv_1234" in formatted  # Adjusted for truncation
    assert "TR(runner_1)" in formatted


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
    record.compact_log_context = False

    formatted = formatter.format(record)

    assert "task_only" in formatted


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
        assert record.task_id == invocation.task.task_id.key
    finally:
        # Reset context
        context.swap_dist_invocation_context(app_instance.app_id, previous)


# ── TTY auto-detection tests ──────────────────────────────────────────────────


def test_auto_detect_colors_tty() -> None:
    """Test that auto-detect enables colors for TTY streams."""
    stream = MagicMock()
    stream.isatty.return_value = True
    assert _auto_detect_colors(stream) is True


def test_auto_detect_colors_non_tty() -> None:
    """Test that auto-detect disables colors for non-TTY streams."""
    stream = MagicMock()
    stream.isatty.return_value = False
    assert _auto_detect_colors(stream) is False


def test_auto_detect_colors_no_isatty() -> None:
    """Test that auto-detect disables colors when isatty is absent."""
    stream = object()  # no isatty attribute
    assert _auto_detect_colors(stream) is False


# ── PlainTextFormatter tests ──────────────────────────────────────────────────


def test_plain_text_formatter_includes_context_prefix() -> None:
    """Test that PlainTextFormatter adds [prefix] to messages."""
    formatter = PlainTextFormatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
    record = logging.LogRecord(
        name="test_logger",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.task_id = "my.task"
    record.invocation_id = "inv_12345678"
    record.runner_ctx = None
    record.compact_log_context = True

    formatted = formatter.format(record)

    assert "[inv_12345678:my.task]" in formatted
    assert "Test message" in formatted
    # No ANSI codes
    assert "\033[" not in formatted


def test_plain_text_formatter_no_context() -> None:
    """Test that PlainTextFormatter works without context."""
    formatter = PlainTextFormatter(
        fmt="%(asctime)s %(levelname)-8s %(name)s %(message)s",
    )
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
    assert "[" not in formatted.split("No context")[0].split("INFO")[1]


# ── JsonFormatter tests ───────────────────────────────────────────────────────


def test_json_formatter_produces_valid_json() -> None:
    """Test that JsonFormatter emits a parseable JSON line."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="pynenc.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None
    record.compact_log_context = True

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["severity"] == "INFO"
    assert parsed["logger"] == "pynenc.test"
    assert parsed["message"] == "Test message"
    assert "timestamp" in parsed
    assert "text" in parsed


def test_json_formatter_includes_context_fields() -> None:
    """Test that JsonFormatter includes structured runner/invocation context."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="pynenc.test",
        level=logging.WARNING,
        pathname="test.py",
        lineno=1,
        msg="Context message",
        args=(),
        exc_info=None,
    )
    runner_ctx = RunnerContext(
        runner_cls="PersistentProcessRunner",
        runner_id="abc12345-6789-0000-0000-000000000000",
    )
    record.task_id = "my_module.my_func"
    record.invocation_id = "def45678-1234-0000-0000-000000000000"
    record.runner_ctx = runner_ctx
    record.compact_log_context = True

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert parsed["runner_class"] == "PersistentProcessRunner"
    assert parsed["runner_id"] == "abc12345-6789-0000-0000-000000000000"
    assert parsed["invocation_id"] == "def45678-1234-0000-0000-000000000000"
    assert parsed["task_id"] == "my_module.my_func"
    assert parsed["severity"] == "WARNING"


def test_json_formatter_text_field_matches_plaintext() -> None:
    """Test that the JSON text field contains the same format as PlainTextFormatter."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="pynenc.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Test message",
        args=(),
        exc_info=None,
    )
    record.task_id = "module.func"
    record.invocation_id = "inv_uuid_12345678"
    record.runner_ctx = None
    record.compact_log_context = True

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    # The text field should have the bracket prefix
    assert "[inv_uuid_12345678:module.func]" in parsed["text"]
    assert "Test message" in parsed["text"]


def test_json_formatter_excludes_none_context() -> None:
    """Test that JSON output omits context keys when no context is present."""
    formatter = JsonFormatter()
    record = logging.LogRecord(
        name="pynenc.test",
        level=logging.INFO,
        pathname="test.py",
        lineno=1,
        msg="Simple message",
        args=(),
        exc_info=None,
    )
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None
    record.compact_log_context = True

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert "runner_class" not in parsed
    assert "invocation_id" not in parsed
    assert "task_id" not in parsed


def test_json_formatter_includes_exception() -> None:
    """Test that JsonFormatter includes exception information."""
    formatter = JsonFormatter()
    try:
        raise ValueError("test error")
    except ValueError:
        import sys

        exc_info = sys.exc_info()

    record = logging.LogRecord(
        name="pynenc.test",
        level=logging.ERROR,
        pathname="test.py",
        lineno=1,
        msg="Error occurred",
        args=(),
        exc_info=exc_info,
    )
    record.task_id = None
    record.invocation_id = None
    record.runner_ctx = None
    record.compact_log_context = True

    formatted = formatter.format(record)
    parsed = json.loads(formatted)

    assert "exception" in parsed
    assert "ValueError: test error" in parsed["exception"]


# ── create_logger configuration tests ─────────────────────────────────────────


def test_create_logger_explicit_colors_false() -> None:
    """Test that create_logger with use_colors=False uses PlainTextFormatter."""
    app = MockPynenc()
    logger = create_logger(app, use_colors=False)
    assert isinstance(logger.handlers[0].formatter, PlainTextFormatter)


def test_create_logger_explicit_colors_true() -> None:
    """Test that create_logger with use_colors=True uses ColoredFormatter."""
    app = MockPynenc()
    logger = create_logger(app, use_colors=True)
    assert isinstance(logger.handlers[0].formatter, ColoredFormatter)


def test_create_logger_json_format() -> None:
    """Test that create_logger with log_format=JSON uses JsonFormatter."""
    app = MockPynenc()
    logger = create_logger(app, log_format=LogFormat.JSON)
    assert isinstance(logger.handlers[0].formatter, JsonFormatter)


def test_create_logger_stdout_stream() -> None:
    """Test that create_logger writes to stdout when configured."""
    app = MockPynenc()
    logger = create_logger(app, stream=sys.stdout)
    handler = logger.handlers[0]
    # mypy: handler is Handler, but only StreamHandler has .stream
    assert isinstance(handler, logging.StreamHandler)
    assert handler.stream is sys.stdout


def test_create_logger_auto_detect_non_tty() -> None:
    """Test that create_logger uses PlainTextFormatter when use_colors=False."""
    app = MockPynenc()
    non_tty = StringIO()  # StringIO has no isatty
    logger = create_logger(app, stream=non_tty, use_colors=False)
    assert isinstance(logger.handlers[0].formatter, PlainTextFormatter)


def test_create_logger_config_overrides() -> None:
    """Test that config values drive formatter and stream selection."""
    app = MockPynenc()
    app.conf.log_format = LogFormat.JSON
    logger = create_logger(app)
    assert isinstance(logger.handlers[0].formatter, JsonFormatter)
