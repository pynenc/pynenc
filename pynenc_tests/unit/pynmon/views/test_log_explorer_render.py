"""Unit tests for pynmon.views.log_explorer_render module.

Tests HTML rendering of parsed log lines including timestamp dimming,
level colorization, bracket linkification, and inline entity linking.

Key components:
- render_log_html: Main rendering entry point
- entity_link_url: URL resolver for entity references
- _split_header_message: Header/message splitting
- _bracket_link_runners / _bracket_link_uuid / _bracket_link_task: Bracket linkification
- _linkify_entities: Message body entity linking
"""

from dataclasses import dataclass, field

import pytest

pytest.importorskip("fastapi", reason="pynmon tests require monitor dependencies")
pytest.importorskip("jinja2", reason="pynmon tests require monitor dependencies")

# ruff: noqa: E402
from pynmon.util.log_parser import ParsedLogLine, ParsedRunner
from pynmon.views.log_explorer_render import (
    _bracket_link_runners,
    _bracket_link_task,
    _bracket_link_uuid,
    _colourize_level,
    _dim_timestamp,
    _linkify_entities,
    _render_header,
    _render_message,
    _split_header_message,
    _sub_outside_tags,
    entity_link_url,
    render_log_html,
)


# Minimal stand-in for LineAnalysis to avoid importing the full log_explorer module
# (which triggers FastAPI router setup, templates, etc.)
@dataclass
class _FakeLineAnalysis:
    parsed: ParsedLogLine
    runner_id_map: dict[str, str] = field(default_factory=dict)


# ################################################################################### #
# ENTITY_LINK_URL
# ################################################################################### #


def test_entity_link_url_should_return_invocation_url() -> None:
    """Invocation kinds map to /invocations/<id>."""
    for kind in (
        "invocation",
        "parent-invocation",
        "child-invocation",
        "new-invocation",
    ):
        url = entity_link_url(kind, "abc-123")
        assert url == "/invocations/abc-123", f"Failed for kind={kind}"


def test_entity_link_url_should_return_runner_url() -> None:
    """Runner/worker kinds map to /runners/<id>."""
    for kind in ("runner", "worker", "current-owner-runner", "attempted-owner-runner"):
        url = entity_link_url(kind, "runner-id")
        assert url == "/runners/runner-id", f"Failed for kind={kind}"


def test_entity_link_url_should_return_task_url() -> None:
    """Task kind maps to /tasks/<key>."""
    assert entity_link_url("task", "my_module.do_work") == "/tasks/my_module.do_work"


def test_entity_link_url_should_return_workflow_url() -> None:
    """Workflow kinds map to /workflows/runs."""
    for kind in ("workflow", "sub-workflow", "parent-workflow"):
        url = entity_link_url(kind, "wf-id")
        assert url == "/workflows/runs", f"Failed for kind={kind}"


def test_entity_link_url_should_return_empty_for_unknown_kind() -> None:
    """Unknown entity kinds return empty string."""
    assert entity_link_url("unknown", "val") == ""


# ################################################################################### #
# _SPLIT_HEADER_MESSAGE
# ################################################################################### #


def test_split_header_message_should_separate_header_and_body() -> None:
    """Standard log line splits into header and message."""
    raw = (
        "2025-01-15 12:30:45.123+00:00 INFO  pynenc.task "
        "[MTR(32002c1a).TR(1829f97b)uuid:module.func] Task started successfully"
    )
    header, msg = _split_header_message(raw)

    assert "2025-01-15" in header
    assert "INFO" in header
    assert "pynenc.task" in header
    assert "[" in header
    assert msg == "Task started successfully"


def test_split_header_message_should_handle_no_bracket() -> None:
    """Log line without bracket still splits correctly."""
    raw = "2025-01-15 12:30:45+00:00 ERROR pynenc.task Something broke"
    header, msg = _split_header_message(raw)

    assert "2025-01-15" in header
    assert msg == "Something broke"


def test_split_header_message_should_return_full_line_when_no_match() -> None:
    """Non-log text returns as header with empty message."""
    raw = "not a log line"
    header, msg = _split_header_message(raw)

    assert header == "not a log line"
    assert msg == ""


# ################################################################################### #
# _DIM_TIMESTAMP
# ################################################################################### #


def test_dim_timestamp_should_wrap_timestamp_in_span() -> None:
    """Leading timestamp gets a log-ts CSS class span."""
    html = "2025-01-15 12:30:45.123+00:00 INFO pynenc msg"
    result = _dim_timestamp(html)

    assert '<span class="log-ts">2025-01-15 12:30:45.123+00:00</span>' in result
    assert "INFO pynenc msg" in result


def test_dim_timestamp_should_not_modify_text_without_timestamp() -> None:
    """Text without a leading timestamp is unchanged."""
    html = "no timestamp here"
    assert _dim_timestamp(html) == html


# ################################################################################### #
# _COLOURIZE_LEVEL
# ################################################################################### #


def test_colourize_level_should_wrap_info() -> None:
    """INFO level gets log-level-info class."""
    result = _colourize_level("some INFO text")
    assert 'class="log-level log-level-info"' in result
    assert ">INFO</span>" in result


def test_colourize_level_should_wrap_error() -> None:
    """ERROR level gets log-level-error class."""
    result = _colourize_level("some ERROR text")
    assert "log-level-error" in result


def test_colourize_level_should_wrap_all_levels() -> None:
    """All five standard levels are colorized."""
    for level, css in [
        ("DEBUG", "log-level-debug"),
        ("INFO", "log-level-info"),
        ("WARNING", "log-level-warning"),
        ("ERROR", "log-level-error"),
        ("CRITICAL", "log-level-critical"),
    ]:
        result = _colourize_level(f"prefix {level} suffix")
        assert css in result, f"Missing CSS class for {level}"


def test_colourize_level_should_not_modify_text_without_level() -> None:
    """Text without a log level keyword is unchanged."""
    html = "no level here"
    assert _colourize_level(html) == html


# ################################################################################### #
# _BRACKET_LINK_RUNNERS
# ################################################################################### #


def test_bracket_link_runners_should_create_runner_links() -> None:
    """Runner abbreviations are wrapped with hyperlinks."""
    content = "MTR(32002c1a).TR(1829f97b)"
    rid_map = {"32002c1a": "full-runner-id-1", "1829f97b": "full-runner-id-2"}
    result = _bracket_link_runners(content, rid_map)

    assert "/runners/full-runner-id-1" in result
    assert "/runners/full-runner-id-2" in result
    assert "MTR(32002c1a)" in result
    assert "TR(1829f97b)" in result


def test_bracket_link_runners_should_use_partial_id_as_fallback() -> None:
    """When no rid_map entry, the partial ID is used in the link."""
    content = "TR(abcdef12)"
    result = _bracket_link_runners(content, {})

    assert "/runners/abcdef12" in result
    assert "TR(abcdef12)" in result


# ################################################################################### #
# _BRACKET_LINK_UUID
# ################################################################################### #


def test_bracket_link_uuid_should_create_invocation_link() -> None:
    """UUID in bracket content is linked to invocation page."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    result = _bracket_link_uuid(uid)

    assert f"/invocations/{uid}" in result
    assert "bracket-invocation" in result


def test_bracket_link_uuid_should_not_modify_text_without_uuid() -> None:
    """Text without a UUID is unchanged."""
    content = "no-uuid-here"
    result = _bracket_link_uuid(content)
    assert result == content


# ################################################################################### #
# _BRACKET_LINK_TASK
# ################################################################################### #


def test_bracket_link_task_should_create_task_link() -> None:
    """Task key after colon is linked to task page."""
    content = ":my_module.my_func"
    result = _bracket_link_task(content)

    assert "/tasks/my_module.my_func" in result
    assert "bracket-task" in result


def test_bracket_link_task_should_not_modify_text_without_colon() -> None:
    """Text without a :task_key pattern is unchanged."""
    content = "no task here"
    result = _bracket_link_task(content)
    assert result == content


# ################################################################################### #
# _SUB_OUTSIDE_TAGS
# ################################################################################### #


def test_sub_outside_tags_should_replace_outside_html_tags() -> None:
    """Substitution applies to text nodes, not inside <tag> definitions."""
    import re

    pattern = re.compile(r"(foo)")
    # "foo" inside an attribute (href="foo") should NOT be replaced
    result = _sub_outside_tags(pattern, lambda m: "bar", '<a href="foo">foo</a>')

    assert 'href="foo"' in result  # attribute preserved
    assert ">bar</a>" in result  # text content replaced


def test_sub_outside_tags_should_preserve_tag_attributes() -> None:
    """Regex patterns inside HTML tag attributes are not modified."""
    import re

    pattern = re.compile(r"(data)")
    result = _sub_outside_tags(
        pattern, lambda m: "REPLACED", '<span data-id="data">data</span>'
    )

    # "data" inside <span data-id="data"> tag definition should remain
    assert 'data-id="data"' in result
    # "data" as text content should be replaced
    assert ">REPLACED</span>" in result


# ################################################################################### #
# _LINKIFY_ENTITIES
# ################################################################################### #


def test_linkify_entities_should_link_invocation_ref() -> None:
    """invocation:UUID tokens in message become hyperlinks."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    html = f"Processing invocation:{uid} now"
    result = _linkify_entities(html)

    assert f"/invocations/{uid}" in result
    assert "log-entity-link" in result
    assert "data-invocation-id" in result


def test_linkify_entities_should_link_runner_ref() -> None:
    """runner:ID tokens become hyperlinks with data-runner-id."""
    html = "Assigned to runner:my-runner-123"
    result = _linkify_entities(html)

    assert "/runners/my-runner-123" in result
    assert "data-runner-id" in result


def test_linkify_entities_should_link_task_ref() -> None:
    """task:KEY tokens become hyperlinks."""
    html = "Registered task:my_module.do_work"
    result = _linkify_entities(html)

    assert "/tasks/my_module.do_work" in result


def test_linkify_entities_should_link_workflow_ref() -> None:
    """workflow:ID tokens become hyperlinks."""
    html = "Starting workflow:wf-abc-123"
    result = _linkify_entities(html)

    assert "/workflows/runs" in result


def test_linkify_entities_should_handle_no_entities() -> None:
    """Text without entity refs is unchanged."""
    html = "just a plain message"
    assert _linkify_entities(html) == html


# ################################################################################### #
# _RENDER_HEADER / _RENDER_MESSAGE
# ################################################################################### #


def test_render_header_should_escape_html() -> None:
    """HTML special characters in header are escaped."""
    header = "2025-01-15 12:30:45+00:00 INFO pynenc <script>alert(1)</script>"
    result = _render_header(header)

    assert "<script>" not in result
    assert "&lt;script&gt;" in result


def test_render_message_should_escape_and_linkify() -> None:
    """Message body is escaped and entity refs are linked."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    msg = f"<b>Bold</b> invocation:{uid}"
    result = _render_message(msg)

    assert "&lt;b&gt;" in result
    assert f"/invocations/{uid}" in result


def test_render_message_should_return_empty_for_empty_input() -> None:
    """Empty message returns empty string."""
    assert _render_message("") == ""


# ################################################################################### #
# RENDER_LOG_HTML (integration)
# ################################################################################### #


def test_render_log_html_should_produce_full_html() -> None:
    """Full rendering combines header + message with log-msg-text div."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    raw = (
        f"2025-01-15 12:30:45+00:00 INFO  pynenc.task "
        f"[TR(abc12345){uid}:mod.fn] invocation:{uid} completed"
    )
    parsed = ParsedLogLine(
        raw=raw,
        runners=[ParsedRunner(cls_abbr="TR", partial_id="abc12345")],
        invocation_id=uid,
        task_key="mod.fn",
        raw_bracket=f"TR(abc12345){uid}:mod.fn",
        is_valid=True,
    )
    la = _FakeLineAnalysis(
        parsed=parsed,
        runner_id_map={"abc12345": "full-runner-abc12345"},
    )
    result = render_log_html(la)  # type: ignore[arg-type]

    # Structure: header + div.log-msg-text with message
    assert "log-msg-text" in result
    # Timestamp dimmed
    assert "log-ts" in result
    # Level colorized
    assert "log-level-info" in result
    # Runner linked
    assert "/runners/full-runner-abc12345" in result
    # Invocation linked (in bracket)
    assert f"/invocations/{uid}" in result
    # Message entity linked
    assert "log-entity-link" in result


def test_render_log_html_should_return_empty_for_empty_raw() -> None:
    """Empty raw line produces empty HTML."""
    parsed = ParsedLogLine(raw="")
    la = _FakeLineAnalysis(parsed=parsed)
    assert render_log_html(la) == ""  # type: ignore[arg-type]


def test_render_log_html_should_handle_line_without_bracket() -> None:
    """Log line without bracket still renders header and message."""
    raw = "2025-01-15 12:30:45+00:00 ERROR pynenc.task Some error occurred"
    parsed = ParsedLogLine(raw=raw, level="ERROR", is_valid=False)
    la = _FakeLineAnalysis(parsed=parsed)
    result = render_log_html(la)  # type: ignore[arg-type]

    assert "log-level-error" in result
    assert "Some error occurred" in result
