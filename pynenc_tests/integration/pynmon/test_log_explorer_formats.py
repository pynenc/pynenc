"""
Integration tests for pynmon log explorer with different logging configurations.

Tests that the log explorer correctly parses and renders logs from every
builder logging combination: format (text/json), colors (on/off), stream
(stdout/stderr). One app + task per config, simple parametrized matrix.

Key components:
- One Pynenc app per logging config, built with PynencBuilder
- Log capture via StringIO stream override
- Log parser and log explorer rendering verification
"""

import io
import logging
import threading
import urllib.parse
from typing import TYPE_CHECKING

import pytest

from pynenc.builder import PynencBuilder

from pynmon.util.log_parser import parse_log_lines

if TYPE_CHECKING:
    from pynenc_tests.integration.pynmon.conftest import PynmonClient
    from pynenc.task import Task

KEEP_ALIVE = 0

# ── one app per logging config ────────────────────────────────────────────────
app_0 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("text-nocolor-stderr")
    .logging(fmt="text", colors=False, stream="stderr")
    .build()
)
app_1 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("text-nocolor-stdout")
    .logging(fmt="text", colors=False, stream="stdout")
    .build()
)
app_2 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("text-color-stderr")
    .logging(fmt="text", colors=True, stream="stderr")
    .build()
)
app_3 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("text-color-stdout")
    .logging(fmt="text", colors=True, stream="stdout")
    .build()
)
app_4 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("json-nocolor-stderr")
    .logging(fmt="json", colors=False, stream="stderr")
    .build()
)
app_5 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("json-nocolor-stdout")
    .logging(fmt="json", colors=False, stream="stdout")
    .build()
)
app_6 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("json-color-stderr")
    .logging(fmt="json", colors=True, stream="stderr")
    .build()
)
app_7 = (
    PynencBuilder()
    .memory()
    .thread_runner()
    .app_id("json-color-stdout")
    .logging(fmt="json", colors=True, stream="stdout")
    .build()
)

# pynmon_server fixture reads `request.module.app`
app = app_0


# ── one task per app ──────────────────────────────────────────────────────────
@app_0.task
def task_0(name: str) -> str:
    task_0.logger.info(f"hello {name}")
    return f"hi {name}"


@app_1.task
def task_1(name: str) -> str:
    task_1.logger.info(f"hello {name}")
    return f"hi {name}"


@app_2.task
def task_2(name: str) -> str:
    task_2.logger.info(f"hello {name}")
    return f"hi {name}"


@app_3.task
def task_3(name: str) -> str:
    task_3.logger.info(f"hello {name}")
    return f"hi {name}"


@app_4.task
def task_4(name: str) -> str:
    task_4.logger.info(f"hello {name}")
    return f"hi {name}"


@app_5.task
def task_5(name: str) -> str:
    task_5.logger.info(f"hello {name}")
    return f"hi {name}"


@app_6.task
def task_6(name: str) -> str:
    task_6.logger.info(f"hello {name}")
    return f"hi {name}"


@app_7.task
def task_7(name: str) -> str:
    task_7.logger.info(f"hello {name}")
    return f"hi {name}"


# ── test matrix (task carries its app via task.app) ───────────────────────────
_TASKS = [task_0, task_1, task_2, task_3, task_4, task_5, task_6, task_7]
_IDS = [t.app.app_id for t in _TASKS]


def _run_and_capture(task_fn: "Task") -> tuple[str, str]:
    """Run *task_fn* and return (log_text, invocation_id) in pynenc's log format."""
    buf = io.StringIO()
    # Redirect the app's existing handler stream to buf, preserving the
    # configured formatter (text/json/colors) so parse_log_lines works.
    stream_handlers = [
        h for h in task_fn.app.logger.handlers if isinstance(h, logging.StreamHandler)
    ]
    original_streams = [(h, h.stream) for h in stream_handlers]
    for handler in stream_handlers:
        handler.setStream(buf)

    runner_thread = threading.Thread(target=task_fn.app.runner.run, daemon=True)
    runner_thread.start()
    try:
        inv = task_fn("world")
        _ = inv.result
        invocation_id = inv.invocation_id
    finally:
        task_fn.app.runner.stop_runner_loop()
        runner_thread.join(timeout=5)
        for handler, original_stream in original_streams:
            handler.setStream(original_stream)
    return buf.getvalue(), invocation_id


# ── tests ──────────────────────────────────────────────────────────────────────
@pytest.mark.parametrize("task_fn", _TASKS, ids=_IDS)
def test_log_parser_extracts_context(task_fn: "Task") -> None:
    """Log parser extracts runner, invocation, and task from every format."""
    label = task_fn.app.app_id
    log_text, invocation_id = _run_and_capture(task_fn)
    assert log_text, f"No log output for {label}"
    assert invocation_id in log_text, f"Invocation ID not in logs for {label}"

    parsed = parse_log_lines(log_text)
    valid = [p for p in parsed if p.is_valid]
    assert valid, f"No valid lines for {label}\n{log_text[:500]}"
    assert any(p.runners for p in valid), f"No runner context for {label}"
    assert any(p.invocation_id for p in valid), f"No invocation for {label}"
    assert any(p.task_key for p in valid), f"No task key for {label}"


@pytest.mark.parametrize("task_fn", _TASKS, ids=_IDS)
def test_log_explorer_renders(pynmon_client: "PynmonClient", task_fn: "Task") -> None:
    """Log explorer renders parsed chips and references for every config."""
    label = task_fn.app.app_id
    log_text, invocation_id = _run_and_capture(task_fn)
    assert log_text, f"No log output for {label}"

    response = pynmon_client.get(f"/log-explorer/?log={urllib.parse.quote(log_text)}")
    assert response.status_code == 200
    content = response.text

    # Analysis section rendered (not just echoed-back text)
    assert "Parsed Log Lines" in content, f"No parsed section for {label}"

    # Runner chip rendered with link
    assert "chip-runner" in content, f"No runner chip for {label}"

    # Invocation chip rendered with data attribute
    assert f'data-invocation-id="{invocation_id}"' in content, (
        f"No invocation chip for {label}"
    )

    # Task chip rendered with link to task page
    assert "chip-task" in content, f"No task chip for {label}"

    # References panel rendered with at least one section
    assert "refs-section" in content, f"No references panel for {label}"
