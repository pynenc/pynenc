import logging
from collections.abc import MutableMapping
from typing import TYPE_CHECKING, Any, Literal, Optional

if TYPE_CHECKING:
    from logging import LogRecord

    from pynenc.app import Pynenc


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

        Args:
            fmt: The format string for the message
            datefmt: The format string for datetime objects
            style: The style of the fmt string
            validate: Whether to validate the format string
        """
        super().__init__(fmt=fmt, datefmt=datefmt, style=style, validate=validate)

    def format(self, record: "LogRecord") -> str:
        # Save original values to restore them after formatting
        levelname = record.levelname
        name = record.name
        msg = record.msg

        # Apply colors
        if levelname in self.LEVEL_COLORS:
            color = self.LEVEL_COLORS[levelname]
            record.levelname = f"{color}{levelname:<8}{Colors.RESET}"
            record.name = f"{Colors.BLUE}{name}{Colors.RESET}"

            # Only color the prefix part of the message for adapters
            if isinstance(record.msg, str) and record.msg.startswith("["):
                # Find the closing bracket
                closing_bracket = record.msg.find("]")
                if closing_bracket != -1:
                    prefix = record.msg[: closing_bracket + 1]
                    rest = record.msg[closing_bracket + 1 :]
                    record.msg = (
                        f"{color}{prefix}{Colors.RESET}{color}{rest}{Colors.RESET}"
                    )
                else:
                    record.msg = f"{color}{record.msg}{Colors.RESET}"
            else:
                record.msg = f"{color}{record.msg}{Colors.RESET}"

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


class TaskLoggerAdapter(logging.LoggerAdapter):
    """
    Logger adapter for tasks.

    This adapter adds task and invocation context to log messages.
    """

    def __init__(
        self, logger: logging.Logger, task_id: str, invocation_id: Optional[str] = None
    ):
        super().__init__(logger, {})
        self.set_context(task_id, invocation_id)

    def set_context(self, task_id: str, invocation_id: Optional[str]) -> None:
        """
        Sets the context for logging.

        :param str task_id: The ID of the task.
        :param Optional[str] invocation_id: The ID of the invocation.
        """
        self.task_id = task_id
        self.invocation_id = invocation_id

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple:
        """
        Processes a log message, adding task and invocation context.

        :param Any msg: The log message.
        :param MutableMapping[str, Any] kwargs: Additional keyword arguments.
        :return: The processed message and kwargs.
        """
        if self.invocation_id:
            prefix = f"[{self.task_id}: {self.invocation_id}]"
        else:
            prefix = f"[{self.task_id}]"
        return f"{prefix} {msg}", kwargs


class RunnerLogAdapter(logging.LoggerAdapter):
    """
    Logger adapter for runners.

    This adapter adds runner context to log messages.

    :param logging.Logger logger: The logger instance.
    :param str runner_id: The ID of the runner.
    """

    def __init__(self, logger: logging.Logger, runner_id: str):
        super().__init__(logger, {})
        self.runner_id = runner_id

    def process(self, msg: Any, kwargs: MutableMapping[str, Any]) -> tuple:
        """
        Processes a log message, adding runner context.

        :param Any msg: The log message.
        :param MutableMapping[str, Any] kwargs: Additional keyword arguments.
        :return: The processed message and kwargs.
        """
        prefix = f"[runner: {self.runner_id}]"
        return f"{prefix} {msg}", kwargs
