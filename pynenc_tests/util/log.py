"""
Test logging helpers.

Provides a simple logger factory used by test code to emit clearly visible
timestamped messages (with a colored test prefix) so test logs are easy to
spot in CI and local runs.

Key components:
- create_test_logger: create a logger with ISO timestamps and a short colored prefix
"""

import logging
from contextlib import contextmanager
from io import StringIO
from typing import Generator


class _Ansi:
    CYAN = "\033[36m"
    RESET = "\033[0m"


def create_test_logger(name: str = "tests") -> logging.Logger:
    """
    Create a logger for tests with ISO timestamp prefix and a colored "[TEST]" prefix.

    :param str name: Logger name
    :return: Configured logging.Logger
    """
    logger = logging.getLogger(f"tests.{name}")
    if logger.handlers:
        return logger

    handler = logging.StreamHandler()

    fmt = "%(asctime)s.%(msecs)03d %(levelname)-8s [TEST] %(name)s %(message)s"

    # Wrapper formatter to inject colored [TEST] prefix only
    class _WrapFormatter(logging.Formatter):
        def format(self, record: logging.LogRecord) -> str:  # type: ignore[override]
            # Timestamp is handled by base formatter; add color to prefix
            original_msg = record.getMessage()
            record.msg = f"{_Ansi.CYAN}[TEST]{_Ansi.RESET} {original_msg}"
            try:
                return super().format(record)
            finally:
                record.msg = original_msg

    handler.setFormatter(_WrapFormatter(fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S"))

    logger.handlers = [handler]
    logger.propagate = False
    logger.setLevel(logging.DEBUG)
    return logger


# @contextmanager
# def capture_logs(name: str = "tests") -> Iterator[list[logging.LogRecord]]:
#     """
#     Context manager to capture logs emitted to tests.<name> logger.

#     :param str name: subname of the test logger (default 'tests')
#     :return: yields list[logging.LogRecord] collected while in context
#     """
#     logger = logging.getLogger(f"tests.{name}")
#     records: List[logging.LogRecord] = []

#     class _ListHandler(logging.Handler):
#         def emit(self, record: logging.LogRecord) -> None:
#             records.append(record)

#     handler = _ListHandler()
#     logger.addHandler(handler)
#     try:
#         yield records
#     finally:
#         logger.removeHandler(handler)


@contextmanager
def capture_logs(
    logger: logging.Logger, level: int = logging.DEBUG
) -> Generator[StringIO, None, None]:
    """Capture logs from a specific logger."""
    log_buffer = StringIO()
    handler = logging.StreamHandler(log_buffer)
    handler.setFormatter(logger.handlers[0].formatter)
    logger.addHandler(handler)

    original_level = logger.level
    logger.setLevel(level)

    try:
        yield log_buffer
    finally:
        logger.removeHandler(handler)
        logger.setLevel(original_level)
