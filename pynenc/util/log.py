"""Logging utilities for Pynenc.

This module provides logging configuration, formatters, and context-aware logging.
The formatter reads execution context from pynenc.context module (single source of truth)
and formats IDs according to configuration.

Key components:
- ColoredFormatter: Adds ANSI colors and context prefixes to log output
- PynencContextFilter: Injects context into log records from pynenc.context
- create_logger: Creates configured logger for a Pynenc app
"""

import logging
from typing import TYPE_CHECKING, Literal

from pynenc import context

if TYPE_CHECKING:
    from logging import LogRecord

    from pynenc.app import Pynenc
    from pynenc.conf.config_pynenc import ConfigPynenc
    from pynenc.runner.runner_context import RunnerContext

# Truncation length for IDs
_TRUNCATE_LENGTH = 8


# Define ANSI color codes
class Colors:
    """ANSI color codes for terminal coloring."""

    RESET = "\033[0m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    CYAN = "\033[36m"
    RED_BG = "\033[41m"
    WHITE = "\033[37m"


class PynencContextFilter(logging.Filter):
    """
    Logging filter that injects Pynenc context into log records.

    Reads execution context from pynenc.context module (single source of truth)
    and adds it to log records for the formatter to use.
    """

    def __init__(self, app_id: str, conf: "ConfigPynenc") -> None:
        """
        Initialize the filter with the app_id to read context for.

        :param str app_id: The application identifier.
        :param ConfigPynenc conf: The app configuration (read dynamically for compact_log_context).
        """
        super().__init__()
        self.app_id = app_id
        self.conf = conf

    def filter(self, record: "LogRecord") -> bool:
        """
        Add context information to the log record.

        Reads from pynenc.context (single source of truth) rather than
        maintaining duplicate state.

        :param LogRecord record: The log record to filter
        :return: Always True to allow the record
        """
        # Read execution context from context.py (single source of truth)
        record.runner_ctx = context.get_current_runner_context(self.app_id)
        if invocation := context.get_dist_invocation_context(self.app_id):
            record.invocation_id = invocation.invocation_id
            record.task_id = invocation.task.task_id.key

        # Read compact_log_context dynamically from config (allows runtime changes)
        record.compact_log_context = self.conf.compact_log_context
        return True


class ColoredFormatter(logging.Formatter):
    """
    Custom formatter that adds colors to log output using ANSI color codes.
    No external dependencies required.
    """

    LEVEL_COLORS = {
        "DEBUG": Colors.CYAN,
        "INFO": Colors.GREEN,
        "WARNING": Colors.YELLOW,
        "ERROR": Colors.RED,
        "CRITICAL": f"{Colors.WHITE}{Colors.RED_BG}",
    }

    def __init__(
        self,
        fmt: str | None = None,
        datefmt: str | None = None,
        style: Literal["%", "{", "$"] = "%",
        validate: bool = True,
    ) -> None:
        """Initialize the formatter with specified format strings.

        :param str | None fmt: The format string for the message
        :param str | None datefmt: The format string for datetime objects
        :param Literal["%", "{", "$"] style: The style of the fmt string
        :param bool validate: Whether to validate the format string
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)

    def format(self, record: "LogRecord") -> str:
        """
        Format the log record with colors and context prefix.

        :param LogRecord record: The log record to format
        :return: Formatted log string
        """
        # Save original values to restore them after formatting
        levelname = record.levelname
        name = record.name
        msg = record.msg

        # Extract raw context data (added by PynencContextFilter from context.py)
        runner_ctx = getattr(record, "runner_ctx", None)
        task_id = getattr(record, "task_id", None)
        invocation_id = getattr(record, "invocation_id", None)
        truncate_ids = getattr(record, "compact_log_context", True)

        # Build context prefix
        prefix = ""

        # Format the context display from raw data
        if context_display := self._format_runner_ctx_display(runner_ctx, truncate_ids):
            prefix = context_display

        # Add invocation context if available
        if invocation_id:
            inv_display = self._maybe_truncate(invocation_id, truncate_ids)
            prefix += inv_display

        # Add task context if available
        if task_id:
            if invocation_id:
                prefix += ":"
            prefix += task_id

        # Apply colors
        if levelname in self.LEVEL_COLORS:
            color = self.LEVEL_COLORS[levelname]
            record.levelname = f"{color}{levelname:<8}{Colors.RESET}"
            record.name = f"{Colors.BLUE}{name}{Colors.RESET}"
            # Add colored context prefix if any context is available
            if prefix:
                record.msg = (
                    f"{color}[{prefix}]{Colors.RESET} {color}{msg}{Colors.RESET}"
                )
            else:
                record.msg = f"{color}{msg}{Colors.RESET}"

        # Format the record
        result = super().format(record)

        # Restore original values
        record.levelname = levelname
        record.name = name
        record.msg = msg

        return result

    def _format_runner_ctx_display(
        self,
        runner_ctx: "RunnerContext | None",
        compact_context: bool,
    ) -> str | None:
        """
        Format context data into a display string for logging.

        Builds hierarchical display from RunnerContext chain:
        - Single context: RunnerClass(id)
        - With parent: ParentClass(id).ChildClass[id]

        :param RunnerContext | None runner_ctx: The current runner context
        :param bool compact_context: Whether to compact class names
        :return: Formatted display string or None if no context
        """
        if runner_ctx is None:
            return None
        curr_cls = self._maybe_compact_class_name(
            runner_ctx.runner_cls, compact_context
        )
        if "External" in runner_ctx.runner_cls:
            # Don't truncate external runner ID (hostname+PID)
            curr_id = runner_ctx.runner_id
        else:
            curr_id = self._maybe_truncate(runner_ctx.runner_id, compact_context)
        if runner_ctx.parent_ctx is not None:
            # Two levels: parent -> current
            parent = runner_ctx.parent_ctx
            parent_cls = self._maybe_compact_class_name(
                parent.runner_cls, compact_context
            )
            parent_id = self._maybe_truncate(parent.runner_id, compact_context)
            return f"{parent_cls}({parent_id}).{curr_cls}({curr_id})"

        # Just current context
        return f"{curr_cls}({curr_id})"

    def _maybe_truncate(self, value: str, truncate: bool) -> str:
        """
        Truncate a string to 7 chars (like Git short SHA) if truncate is True.

        :param str value: The string to potentially truncate
        :param bool truncate: Whether to truncate
        :return: Original or truncated string
        """
        if truncate and len(value) > _TRUNCATE_LENGTH:
            return value[:_TRUNCATE_LENGTH]
        return value

    def _maybe_compact_class_name(self, class_name: str, compact: bool) -> str:
        """
        Abbreviate class name to capital letters if compact is True.

        Examples: PersistentProcessRunner -> PPR, PPRWorker -> PPRW

        :param str class_name: The class name to potentially compact
        :param bool compact: Whether to compact
        :return: Original or compacted class name
        """
        if not compact:
            return class_name

        return "".join(char for char in class_name if char.isupper())


def create_logger(app: "Pynenc", use_colors: bool = True) -> logging.Logger:
    """
    Creates a logger for the specified app with timestamps and optional colored output.

    :param Pynenc app: The app instance for which the logger is created.
    :param bool use_colors: Whether to use colored output (defaults to True).
    :return: The created logger.
    :raises ValueError: If the logging level is invalid.
    """
    logger = logging.getLogger(f"pynenc.{app.app_id}")

    # Add context filter that reads from context.py
    # Pass conf reference so compact_log_context is read dynamically
    context_filter = PynencContextFilter(app.app_id, app.conf)
    logger.addFilter(context_filter)

    # Create handler
    handler = logging.StreamHandler()

    # Create formatter with timestamp
    if use_colors:
        formatter: logging.Formatter = ColoredFormatter(
            fmt="%(asctime)s.%(msecs)03d %(levelname)s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    else:
        formatter = logging.Formatter(
            fmt="%(asctime)s.%(msecs)03d %(levelname)-8s %(name)s %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )

    # Set formatter to handler
    handler.setFormatter(formatter)

    # Remove any existing handlers and add our new one
    logger.handlers = [handler]

    # Set level
    if level_name := app.conf.logging_level:
        numeric_level = getattr(logging, level_name.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {level_name}")
        logger.setLevel(numeric_level)

    # Prevent propagation to root logger to avoid duplicate logs
    logger.propagate = False

    return logger
