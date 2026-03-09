"""Logging utilities for Pynenc.

Context-aware logging with three output formats:

- **ColoredFormatter**: ANSI-colored terminal output with context prefixes
- **PlainTextFormatter**: Plain text for non-TTY / log files
- **JsonFormatter**: Structured JSON for containers and cloud log aggregators

Timestamps include the local timezone offset (e.g. ``2026-03-05 07:56:07.476+01:00``).

Key components:
- PynencContextFilter: Injects runner/invocation context into log records
- _PynencFormatterBase: Shared TZ-aware timestamps and context prefix logic
- create_logger: Factory that wires formatter, stream, and filter from config
"""

import json
import logging
import sys
import time as _time
from typing import TYPE_CHECKING, Any

from pynenc import context
from pynenc.conf.config_pynenc import LogFormat

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
    """Injects Pynenc execution context into every log record."""

    def __init__(self, app_id: str, conf: "ConfigPynenc") -> None:
        """
        :param str app_id: The application identifier.
        :param ConfigPynenc conf: App configuration (read dynamically).
        """
        super().__init__()
        self.app_id = app_id
        self.conf = conf

    def filter(self, record: "LogRecord") -> bool:
        """Populate record with runner_ctx, invocation_id, task_id, compact_log_context."""
        record.runner_ctx = context.get_current_runner_context(self.app_id)
        if invocation := context.get_dist_invocation_context(self.app_id):
            record.invocation_id = invocation.invocation_id
            record.task_id = invocation.task.task_id.key
        record.compact_log_context = self.conf.compact_log_context
        return True


class _PynencFormatterBase(logging.Formatter):
    """Base formatter: TZ-aware timestamps, context prefix building, ID truncation."""

    def formatTime(self, record: "LogRecord", datefmt: str | None = None) -> str:
        """Produce ``YYYY-MM-DD HH:MM:SS.mmm±HH:MM`` timestamp."""
        ct = self.converter(record.created)
        base = _time.strftime("%Y-%m-%d %H:%M:%S", ct)
        return f"{base}.{int(record.msecs):03d}{_format_tz_offset(ct)}"

    def _build_context_prefix(self, record: "LogRecord") -> str:
        """Build ``CLS(id)UUID:task.key`` prefix from record context attributes."""
        runner_ctx = getattr(record, "runner_ctx", None)
        task_id = getattr(record, "task_id", None)
        invocation_id = getattr(record, "invocation_id", None)
        truncate_ids = getattr(record, "compact_log_context", True)

        prefix = ""
        if context_display := self._format_runner_ctx_display(runner_ctx, truncate_ids):
            prefix = context_display
        if invocation_id:
            prefix += invocation_id
        if task_id:
            if invocation_id:
                prefix += ":"
            prefix += task_id
        return prefix

    def _build_context_dict(self, record: "LogRecord") -> dict[str, Any]:
        """Extract untruncated context fields as a dict for structured output."""
        runner_ctx: RunnerContext | None = getattr(record, "runner_ctx", None)
        invocation_id: str | None = getattr(record, "invocation_id", None)
        task_id: str | None = getattr(record, "task_id", None)

        result: dict[str, Any] = {}
        if runner_ctx is not None:
            result["runner_class"] = runner_ctx.runner_cls
            result["runner_id"] = runner_ctx.runner_id
            if runner_ctx.parent_ctx is not None:
                result["parent_runner_class"] = runner_ctx.parent_ctx.runner_cls
                result["parent_runner_id"] = runner_ctx.parent_ctx.runner_id
        if invocation_id is not None:
            result["invocation_id"] = invocation_id
        if task_id is not None:
            result["task_id"] = task_id
        return result

    def _format_runner_ctx_display(
        self,
        runner_ctx: "RunnerContext | None",
        compact_context: bool,
    ) -> str | None:
        """Format runner context as ``CLS(id)`` or ``Parent(id).Child(id)``."""
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
        """Truncate to ``_TRUNCATE_LENGTH`` chars when *truncate* is True."""
        if truncate and len(value) > _TRUNCATE_LENGTH:
            return value[:_TRUNCATE_LENGTH]
        return value

    def _maybe_compact_class_name(self, class_name: str, compact: bool) -> str:
        """Abbreviate to capital letters (e.g. PersistentProcessRunner → PPR)."""
        if not compact:
            return class_name

        return "".join(char for char in class_name if char.isupper())


class ColoredFormatter(_PynencFormatterBase):
    """ANSI-colored formatter with context prefixes for interactive terminals."""

    LEVEL_COLORS = {
        "DEBUG": Colors.CYAN,
        "INFO": Colors.GREEN,
        "WARNING": Colors.YELLOW,
        "ERROR": Colors.RED,
        "CRITICAL": f"{Colors.WHITE}{Colors.RED_BG}",
    }

    def format(self, record: "LogRecord") -> str:
        """Format with ANSI colors and ``[context]`` prefix."""
        levelname, name, msg = record.levelname, record.name, record.msg
        prefix = self._build_context_prefix(record)

        if color := self.LEVEL_COLORS.get(levelname):
            record.levelname = f"{color}{levelname:<8}{Colors.RESET}"
            record.name = f"{Colors.BLUE}{name}{Colors.RESET}"
            record.msg = (
                f"{color}[{prefix}]{Colors.RESET} {color}{msg}{Colors.RESET}"
                if prefix
                else f"{color}{msg}{Colors.RESET}"
            )

        result = super().format(record)
        record.levelname, record.name, record.msg = levelname, name, msg
        return result


class PlainTextFormatter(_PynencFormatterBase):
    """Plain text formatter with context prefixes, compatible with pynmon log explorer."""

    def format(self, record: "LogRecord") -> str:
        """Format with ``[context]`` prefix, no ANSI codes."""
        msg = record.msg
        if prefix := self._build_context_prefix(record):
            record.msg = f"[{prefix}] {msg}"
        result = super().format(record)
        record.msg = msg
        return result


class JsonFormatter(_PynencFormatterBase):
    """Structured JSON formatter for containers and cloud log aggregators.

    Uses ``severity`` as the level key (Google Cloud Logging convention, also
    recognized by Datadog, AWS CloudWatch, and ELK). Includes a ``text`` field
    with the human-readable representation for pynmon log explorer compatibility.
    """

    def format(self, record: "LogRecord") -> str:
        """Emit one JSON object per line with structured context fields."""
        timestamp = self.formatTime(record)
        message = record.getMessage()
        prefix = self._build_context_prefix(record)

        # Human-readable text line for pynmon compatibility
        text_parts = [timestamp, f"{record.levelname:<8}", record.name]
        if prefix:
            text_parts.append(f"[{prefix}]")
        text_parts.append(message)

        log_entry: dict[str, Any] = {
            "timestamp": timestamp,
            "severity": record.levelname,
            "logger": record.name,
            "message": message,
            **self._build_context_dict(record),
            "text": " ".join(text_parts),
        }

        if record.exc_info and not record.exc_text:
            record.exc_text = self.formatException(record.exc_info)
        if record.exc_text:
            log_entry["exception"] = record.exc_text
        if record.stack_info:
            log_entry["stack_info"] = self.formatStack(record.stack_info)

        return json.dumps(log_entry, default=str)


def _format_tz_offset(ct: _time.struct_time) -> str:
    """Format timezone offset as ``±HH:MM`` from struct_time."""
    tz = _time.strftime("%z", ct)
    if len(tz) >= 5:
        return f"{tz[:3]}:{tz[3:]}"
    return ""


def _auto_detect_colors(stream: Any) -> bool:
    """Return True if *stream* is an interactive TTY (uvicorn/click/rich convention)."""
    return hasattr(stream, "isatty") and stream.isatty()


def _resolve_stream(log_stream: str) -> Any:
    """Map ``"stdout"``/``"stderr"`` to the corresponding ``sys`` stream."""
    if log_stream == "stdout":
        return sys.stdout
    return sys.stderr


def create_logger(
    app: "Pynenc",
    use_colors: bool | None = None,
    log_format: "LogFormat | str | None" = None,
    stream: Any | None = None,
) -> logging.Logger:
    """
    Create a configured logger for a Pynenc app.

    Resolution order for each setting: explicit parameter → ``app.conf`` → auto-detect.

    :param Pynenc app: The app instance for which the logger is created.
    :param bool | None use_colors: Override color setting (None = auto-detect TTY).
    :param LogFormat | str | None log_format: Override log format (None = from config).
    :param Any | None stream: Override output stream (None = from config).
    :return: The created logger.
    :raises ValueError: If the logging level is invalid.
    """
    logger = logging.getLogger(f"pynenc.{app.app_id}")
    logger.addFilter(PynencContextFilter(app.app_id, app.conf))

    # Resolve stream: parameter → config → stderr
    if stream is None:
        stream = _resolve_stream(app.conf.log_stream)
    handler = logging.StreamHandler(stream=stream)

    # Resolve format: parameter → config
    effective_format = (
        LogFormat(log_format)
        if log_format is not None
        else LogFormat(app.conf.log_format)
    )

    # Resolve colors: parameter → config → TTY auto-detect
    if use_colors is None:
        use_colors = app.conf.log_use_colors
    if use_colors is None:
        use_colors = _auto_detect_colors(stream)

    formatter: logging.Formatter
    match effective_format:
        case LogFormat.JSON:
            formatter = JsonFormatter()
        case LogFormat.TEXT if use_colors:
            formatter = ColoredFormatter(
                fmt="%(asctime)s %(levelname)s %(name)s %(message)s",
            )
        case _:
            formatter = PlainTextFormatter(
                fmt="%(asctime)s %(levelname)-8s %(name)s %(message)s",
            )

    handler.setFormatter(formatter)
    logger.handlers = [handler]

    if level_name := app.conf.logging_level:
        numeric_level = getattr(logging, level_name.upper(), None)
        if not isinstance(numeric_level, int):
            raise ValueError(f"Invalid log level: {level_name}")
        logger.setLevel(numeric_level)

    logger.propagate = False
    return logger
