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
from contextvars import ContextVar
from typing import TYPE_CHECKING, Literal

from pynenc import context

if TYPE_CHECKING:
    from logging import LogRecord

    from pynenc.app import Pynenc
    from pynenc.conf.config_pynenc import ConfigPynenc
    from pynenc.runner.runner_context import RunnerContext

# Truncation length for IDs (7 chars like Git short SHA)
_TRUNCATE_LENGTH = 7

# Context variables for logging-specific data
# These are set directly by invocation.run(), not duplicated from context.py
_task_id: ContextVar[str | None] = ContextVar("task_id", default=None)
_invocation_id: ContextVar[str | None] = ContextVar("invocation_id", default=None)


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
        :param ConfigPynenc conf: The app configuration (read dynamically for truncate_log_ids).
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

        # Read logging-specific context (task/invocation set during run)
        record.task_id = _task_id.get()
        record.invocation_id = _invocation_id.get()
        # Read truncate_log_ids dynamically from config (allows runtime changes)
        record.truncate_log_ids = self.conf.truncate_log_ids
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
        truncate_ids = getattr(record, "truncate_log_ids", True)

        # Build context prefix
        # Format: RunnerClass(id) or RunnerClass(id).Child[id]
        context_parts = []

        # Format the context display from raw data
        if context_display := self._format_context_display(runner_ctx, truncate_ids):
            context_parts.append(context_display)

        # Add task context if available
        if task_id:
            context_parts.append(f"task:{task_id}")

        # Add invocation context if available
        if invocation_id:
            inv_display = self._maybe_truncate(invocation_id, truncate_ids)
            context_parts.append(f"inv:{inv_display}")

        # Apply colors
        if levelname in self.LEVEL_COLORS:
            color = self.LEVEL_COLORS[levelname]
            record.levelname = f"{color}{levelname:<8}{Colors.RESET}"
            record.name = f"{Colors.BLUE}{name}{Colors.RESET}"

            # Add colored context prefix if any context is available
            if context_parts:
                prefix = f"[{' '.join(context_parts)}]"
                record.msg = f"{color}{prefix}{Colors.RESET} {color}{msg}{Colors.RESET}"
            else:
                record.msg = f"{color}{msg}{Colors.RESET}"

        # Format the record
        result = super().format(record)

        # Restore original values
        record.levelname = levelname
        record.name = name
        record.msg = msg

        return result

    def _format_context_display(
        self,
        runner_ctx: "RunnerContext | None",
        truncate_ids: bool,
    ) -> str | None:
        """
        Format context data into a display string for logging.

        Builds hierarchical display from RunnerContext chain:
        - Single context: RunnerClass(id)
        - With parent: ParentClass(id).ChildClass[id]

        :param RunnerContext | None runner_ctx: The current runner context
        :param bool truncate_ids: Whether to truncate long IDs
        :return: Formatted display string or None if no context
        """
        if runner_ctx is None:
            return None

        if runner_ctx.parent_ctx is not None:
            # Two levels: parent -> current
            parent = runner_ctx.parent_ctx
            parent_id = self._maybe_truncate(parent.runner_id, truncate_ids)
            child_id = self._maybe_truncate(runner_ctx.runner_id, truncate_ids)
            return (
                f"{parent.runner_cls}({parent_id}).{runner_ctx.runner_cls}[{child_id}]"
            )

        # Just current context
        runner_id = self._maybe_truncate(runner_ctx.runner_id, truncate_ids)
        return f"{runner_ctx.runner_cls}({runner_id})"

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
    # Pass conf reference so truncate_log_ids is read dynamically
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


def set_logging_context(
    task_id: str | None = None,
    invocation_id: str | None = None,
) -> None:
    """
    Set the task and invocation context for logging.

    These are logging-specific values set during invocation.run(),
    not duplicated from context.py.

    :param str | None task_id: The task ID to set in context
    :param str | None invocation_id: The invocation ID to set in context
    """
    if task_id is not None:
        _task_id.set(task_id)
    if invocation_id is not None:
        _invocation_id.set(invocation_id)


def clear_logging_context() -> None:
    """Clear logging-specific context variables (task_id and invocation_id)."""
    _task_id.set(None)
    _invocation_id.set(None)


def get_logging_context(app_id: str) -> dict[str, str | None]:
    """
    Get the current logging context.

    Reads from context.py for execution context (single source of truth).

    :param str app_id: The application identifier.
    :return: Dictionary with all context values
    """
    # Import here to avoid circular imports
    from pynenc import context

    runner_ctx = context.get_current_runner_context(app_id)
    return {
        "runner_cls": runner_ctx.runner_cls,
        "runner_id": runner_ctx.runner_id,
        "parent_runner_id": runner_ctx.parent_ctx.runner_id
        if runner_ctx.parent_ctx
        else None,
        "task_id": _task_id.get(),
        "invocation_id": _invocation_id.get(),
    }
