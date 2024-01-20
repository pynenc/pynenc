import logging
from typing import TYPE_CHECKING
from unittest.mock import Mock

import pytest

from pynenc.util.log import RunnerLogAdapter, TaskLoggerAdapter, create_logger

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
