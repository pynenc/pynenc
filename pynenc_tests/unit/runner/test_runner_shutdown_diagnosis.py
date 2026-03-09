"""
Tests for shutdown diagnostics: signal classification, log output, and
runner integration. Focus is on real behaviour — not mock coverage.
"""

import signal
import threading
import time
from logging import Logger
from unittest.mock import MagicMock

import pytest

from pynenc.runner.shutdown_diagnostics import (
    ShutdownReason,
    classify_signal,
    log_runner_shutdown,
    _system_info,
)
from pynenc_tests.conftest import MockPynenc
from pynenc_tests.util import capture_logs

app = MockPynenc.with_id("pynenc_tests/unit/runner/test_runner_shutdown_diagnosis")


# ---------------------------------------------------------------------------
# classify_signal
# ---------------------------------------------------------------------------


@pytest.mark.parametrize(
    "signum, expected",
    [
        (None, ShutdownReason.NORMAL),
        (signal.SIGTERM, ShutdownReason.SIGTERM),
        (signal.SIGINT, ShutdownReason.SIGINT),
        (signal.SIGKILL, ShutdownReason.OOM_KILLED),
        (99, ShutdownReason.UNKNOWN_SIGNAL),
    ],
)
def test_classify_signal(signum: int | None, expected: ShutdownReason) -> None:
    assert classify_signal(signum) == expected


# ---------------------------------------------------------------------------
# _system_info
# ---------------------------------------------------------------------------


def test_system_info_contains_required_keys() -> None:
    info = _system_info()
    assert "platform" in info
    assert "python" in info
    assert "cpu_count" in info
    # values are non-empty
    assert info["platform"]
    assert info["python"]


# ---------------------------------------------------------------------------
# log_runner_shutdown — log level and content
# ---------------------------------------------------------------------------


def _make_logger() -> MagicMock:
    logger: MagicMock = MagicMock(spec=Logger)
    return logger


def test_sigterm_uses_warning_not_critical() -> None:
    logger = _make_logger()
    log_runner_shutdown(logger, "TestRunner", "runner-abc", signal.SIGTERM)
    logger.warning.assert_called_once()
    logger.critical.assert_not_called()


def test_sigkill_uses_critical_and_extra_oom_message() -> None:
    logger = _make_logger()
    log_runner_shutdown(logger, "TestRunner", "runner-abc", signal.SIGKILL)
    assert logger.critical.call_count == 2  # diagnostics block + OOM message
    logger.warning.assert_not_called()


def test_log_includes_process_info() -> None:
    logger = _make_logger()
    proc = MagicMock()
    proc.is_alive.return_value = True
    proc.pid = 1234
    log_runner_shutdown(
        logger,
        "TestRunner",
        "runner-abc",
        signal.SIGTERM,
        processes={"runner-xyz": (proc, "inv-001")},
    )
    logged = logger.warning.call_args[0][0]
    assert "ALIVE" in logged
    assert "1234" in logged
    assert "inv-001" in logged


def test_log_includes_thread_info() -> None:
    logger = _make_logger()
    thread = MagicMock()
    thread.is_alive.return_value = False
    thread.name = "worker-1"
    log_runner_shutdown(
        logger,
        "TestRunner",
        "runner-abc",
        signal.SIGTERM,
        threads={"t-key": (thread, "inv-002")},
    )
    logged = logger.warning.call_args[0][0]
    assert "DEAD" in logged
    assert "worker-1" in logged
    assert "inv-002" in logged


def test_log_includes_waiting_invocations() -> None:
    logger = _make_logger()
    log_runner_shutdown(
        logger,
        "TestRunner",
        "runner-abc",
        None,
        waiting_inv_ids=["inv-a", "inv-b"],
    )
    logged = logger.warning.call_args[0][0]
    assert "inv-a" in logged
    assert "inv-b" in logged


def test_log_includes_system_info() -> None:
    logger = _make_logger()
    log_runner_shutdown(logger, "TestRunner", "runner-abc", None)
    logged = logger.warning.call_args[0][0]
    assert "os:" in logged
    assert "py:" in logged
    assert "cpus:" in logged


# ---------------------------------------------------------------------------
# BaseRunner.stop_runner_loop integration
# ---------------------------------------------------------------------------


def test_stop_runner_loop_sets_running_false_and_calls_on_stop() -> None:
    """stop_runner_loop must stop the loop even when _log_shutdown raises."""
    with capture_logs(app.logger) as buf:
        thread = threading.Thread(target=app.runner.run, daemon=True)
        thread.start()
        time.sleep(0.05)
        app.runner.stop_runner_loop(signum=signal.SIGTERM)
        thread.join(timeout=2)

    assert not app.runner.running
    app.runner._on_stop_runner_loop.assert_called_once()
    assert "sigterm" in buf.getvalue()


def test_stop_runner_loop_survives_broken_log_shutdown() -> None:
    """A crash in _log_shutdown must not prevent the loop from stopping."""
    original_log_shutdown = app.runner._log_shutdown
    app.runner._log_shutdown = MagicMock(side_effect=RuntimeError("boom"))  # type: ignore[method-assign]
    try:
        with capture_logs(app.logger) as buf:
            thread = threading.Thread(target=app.runner.run, daemon=True)
            thread.start()
            time.sleep(0.05)
            app.runner.stop_runner_loop(signum=signal.SIGTERM)
            thread.join(timeout=2)

        assert not app.runner.running
        assert "Failed to collect shutdown diagnostics" in buf.getvalue()
    finally:
        app.runner._log_shutdown = original_log_shutdown
