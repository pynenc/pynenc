import logging
from contextvars import ContextVar
from typing import TYPE_CHECKING, Literal

if TYPE_CHECKING:
    from logging import LogRecord

    from pynenc.app import Pynenc


# Context variables for thread-safe logging context
_task_id: ContextVar[str | None] = ContextVar("task_id", default=None)
_invocation_id: ContextVar[str | None] = ContextVar("invocation_id", default=None)
_runner_id: ContextVar[str | None] = ContextVar("runner_id", default=None)


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

    Automatically adds task_id, invocation_id, and runner_id to log records
    based on the current execution context.
    """

    def filter(self, record: "LogRecord") -> bool:
        """
        Add context information to the log record.

        :param LogRecord record: The log record to filter
        :return: Always True to allow the record
        """
        record.task_id = _task_id.get()
        record.invocation_id = _invocation_id.get()
        record.runner_id = _runner_id.get()
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
        # Save original values to restore them after formatting
        levelname = record.levelname
        name = record.name
        msg = record.msg

        # Extract context values, using N/A for missing ones
        task_id = getattr(record, "task_id", None)
        invocation_id = getattr(record, "invocation_id", None)
        runner_id = getattr(record, "runner_id", None)

        # Build context prefix with all available information
        context_parts = []

        # Add runner context if available
        if runner_id:
            context_parts.append(f"runner:{runner_id}")

        # Add task context if available
        if task_id:
            context_parts.append(f"task:{task_id}")

        # Add invocation context if available
        if invocation_id:
            context_parts.append(f"inv:{invocation_id}")

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


def create_logger(app: "Pynenc", use_colors: bool = True) -> logging.Logger:
    """
    Creates a logger for the specified app with timestamps and optional colored output.

    :param Pynenc app: The app instance for which the logger is created.
    :param bool use_colors: Whether to use colored output (defaults to True).
    :return: The created logger.
    :raises ValueError: If the logging level is invalid.
    """
    logger = logging.getLogger(f"pynenc.{app.app_id}")

    # Add context filter
    context_filter = PynencContextFilter()
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
    runner_id: str | None = None,
) -> None:
    """
    Set the logging context for the current execution context.

    :param str | None task_id: The task ID to set in context
    :param str | None invocation_id: The invocation ID to set in context
    :param str | None runner_id: The runner ID to set in context
    """
    if task_id is not None:
        _task_id.set(task_id)
    if invocation_id is not None:
        _invocation_id.set(invocation_id)
    if runner_id is not None:
        _runner_id.set(runner_id)


def clear_logging_context() -> None:
    """Clear all logging context variables."""
    _task_id.set(None)
    _invocation_id.set(None)
    _runner_id.set(None)


def get_logging_context() -> dict[str, str | None]:
    """
    Get the current logging context.

    :return: Dictionary with task_id, invocation_id, and runner_id
    """
    return {
        "task_id": _task_id.get(),
        "invocation_id": _invocation_id.get(),
        "runner_id": _runner_id.get(),
    }
