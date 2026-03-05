"""HTML rendering helpers for the Log Explorer.

Transforms parsed log lines into safe HTML with inline hyperlinks for
runner IDs, invocation UUIDs, task keys, and structured entity references.
Also linkifies truncated IDs inside the ``[bracket]`` context section.

Key components:
- render_log_html: Main rendering function registered as a Jinja2 global
- entity_link_url: URL resolver for entity references
"""

import re
from html import escape
from typing import TYPE_CHECKING
from collections.abc import Callable

from pynmon.app import templates

if TYPE_CHECKING:
    from pynmon.views.log_explorer import LineAnalysis

# ── regex constants ────────────────────────────────────────────────────────────

# Entity:value tokens in the message body for inline linking
_INLINE_ENTITY_RE = re.compile(
    r"((?:invocation|runner|worker|task"
    r"|parent-invocation|child-invocation|new-invocation"
    r"|current-owner-runner|attempted-owner-runner"
    r"|workflow|sub-workflow|parent-workflow)"
    r":[0-9a-zA-Z._-]+(?:-[0-9a-fA-F]{4}){0,4})",
)

# Log-level tokens → CSS class mapping
_LEVEL_CLASSES = {
    "DEBUG": "log-level-debug",
    "INFO": "log-level-info",
    "WARNING": "log-level-warning",
    "ERROR": "log-level-error",
    "CRITICAL": "log-level-critical",
}

# Bracket inner patterns for linkification
_BRACKET_RUNNER_RE = re.compile(r"([A-Z]+)\(([^)]+)\)")
_BRACKET_UUID_RE = re.compile(
    r"([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12})",
    re.IGNORECASE,
)
_BRACKET_TASK_RE = re.compile(r":([a-zA-Z_][a-zA-Z0-9_.]+)")

# Timestamp with optional TZ offset
_TS_RE = re.compile(
    r"^(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?)"
)

# Splits a raw log line into header (timestamp+level+logger+bracket) and message
_HEADER_MSG_RE = re.compile(
    r"^("
    r"\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?"
    r"\s+\w+\s+"  # level
    r"\S+"  # logger name
    r"(?:\s+\[[^\]]*\])?"  # optional bracket
    r"\s*)"
    r"(.+)$",
    re.DOTALL,
)


# ── public helpers ─────────────────────────────────────────────────────────────


def entity_link_url(kind: str, value: str) -> str:
    """Return the URL for an entity reference hyperlink.

    :param str kind: Entity kind like 'invocation', 'runner', 'task', etc.
    :param str value: Entity ID or key
    :return: Relative URL string
    """
    if kind in (
        "invocation",
        "parent-invocation",
        "child-invocation",
        "new-invocation",
    ):
        return f"/invocations/{value}"
    if kind in ("runner", "worker", "current-owner-runner", "attempted-owner-runner"):
        return f"/runners/{value}"
    if kind == "task":
        return f"/tasks/{value}"
    if kind in ("workflow", "sub-workflow", "parent-workflow"):
        return "/workflows/runs"
    return ""


def render_log_html(la: "LineAnalysis") -> str:
    """Render a log line as HTML with header and message on separate lines.

    The header (timestamp + level + logger + bracket) is rendered with
    muted timestamp, coloured level, and bracket links using full runner IDs.
    The message body is rendered on a new line with inline entity links.

    :param LineAnalysis la: The analysed log line
    :return: Safe HTML string for embedding in the template
    """
    raw = la.parsed.raw
    if not raw:
        return ""
    header_raw, msg_raw = _split_header_message(raw)
    header_html = _render_header(header_raw, la.runner_id_map)
    msg_html = _render_message(msg_raw)
    return f'{header_html}<div class="log-msg-text">{msg_html}</div>'


# ── internal rendering steps ───────────────────────────────────────────────────


def _split_header_message(raw: str) -> tuple[str, str]:
    """Split a raw log line into header prefix and message body.

    :param str raw: The raw log line
    :return: Tuple of (header, message); if no split found, header is full line
    """
    if m := _HEADER_MSG_RE.match(raw):
        return m.group(1), m.group(2)
    return raw, ""


def _render_header(header: str, rid_map: dict[str, str] | None = None) -> str:
    """Escape and decorate the header portion (timestamp+level+logger+bracket)."""
    safe = escape(header)
    safe = _dim_timestamp(safe)
    safe = _colourize_level(safe)
    safe = _linkify_bracket(safe, rid_map or {})
    return safe


def _render_message(msg: str) -> str:
    """Escape and linkify the message body."""
    if not msg:
        return ""
    safe = escape(msg)
    return _linkify_entities(safe)


def _colourize_level(html: str) -> str:
    """Wrap the first log-level keyword in a coloured span."""
    for lvl, css_cls in _LEVEL_CLASSES.items():
        pattern = re.compile(rf"\b({re.escape(lvl)})\b")
        result = pattern.sub(
            rf'<span class="log-level {css_cls}">\1</span>',
            html,
            count=1,
        )
        if result != html:
            return result
    return html


def _linkify_bracket(html: str, rid_map: dict[str, str]) -> str:
    """Replace bracket content with individually hyperlinked components."""

    def _replacer(m: re.Match[str]) -> str:
        return _bracket_replacer(m, rid_map)

    return re.sub(r"\[([^\]]+)\]", _replacer, html, count=1)


def _bracket_replacer(m: re.Match[str], rid_map: dict[str, str]) -> str:
    """Build bracketed HTML with runner, invocation, and task links."""
    content = m.group(1)
    content = _bracket_link_runners(content, rid_map)
    content = _bracket_link_uuid(content)
    content = _bracket_link_task(content)
    return f'<span class="log-bracket">[{content}]</span>'


def _bracket_link_runners(content: str, rid_map: dict[str, str]) -> str:
    """Wrap runner abbreviations like PPR(id) with hyperlinks using full IDs."""

    def _repl(rm: re.Match[str]) -> str:
        cls, pid = rm.group(1), rm.group(2)
        full_id = rid_map.get(pid, pid)
        return (
            f'<a href="/runners/{full_id}" class="bracket-link bracket-runner"'
            f' data-runner-id="{full_id}"'
            f' title="Runner: {full_id}">{cls}({pid})</a>'
        )

    return _BRACKET_RUNNER_RE.sub(_repl, content)


def _bracket_link_uuid(content: str) -> str:
    """Wrap UUID invocation IDs with hyperlinks (outside existing tags only)."""

    def _repl(um: re.Match[str]) -> str:
        uid = um.group(1)
        return (
            f'<a href="/invocations/{uid}" class="bracket-link bracket-invocation"'
            f' data-invocation-id="{uid}"'
            f' title="Invocation: {uid}">{uid}</a>'
        )

    return _sub_outside_tags(_BRACKET_UUID_RE, _repl, content)


def _bracket_link_task(content: str) -> str:
    """Wrap task key (after colon) with a hyperlink (outside existing tags only)."""

    def _repl(tm: re.Match[str]) -> str:
        key = tm.group(1)
        return (
            f':<a href="/tasks/{key}" class="bracket-link bracket-task"'
            f' title="Task: {key}">{key}</a>'
        )

    return _sub_outside_tags(_BRACKET_TASK_RE, _repl, content)


def _sub_outside_tags(
    pattern: re.Pattern, repl: Callable[[re.Match[str]], str], text: str
) -> str:
    """Apply regex substitution only to text outside HTML tags.

    Splits text into tag/non-tag parts, applies substitution only to
    non-tag parts to avoid corrupting HTML attributes.
    """
    parts = re.split(r"(<[^>]+>)", text)
    for i, part in enumerate(parts):
        if not part.startswith("<"):
            parts[i] = pattern.sub(repl, part)
    return "".join(parts)


def _linkify_entities(html: str) -> str:
    """Replace entity:value tokens in the message with hyperlinks."""
    return _INLINE_ENTITY_RE.sub(_entity_replacer, html)


def _entity_replacer(m: re.Match[str]) -> str:
    """Build an <a> tag for a single entity token.

    Emits data-runner-id or data-invocation-id so the JS hover wiring can
    cross-highlight SVG bars and other chips on the page.
    """
    token = m.group(1)
    colon_idx = token.index(":")
    kind, value = token[:colon_idx], token[colon_idx + 1 :]
    url = entity_link_url(kind, value)
    if url:
        if kind in (
            "runner",
            "worker",
            "current-owner-runner",
            "attempted-owner-runner",
        ):
            data_attr = f' data-runner-id="{escape(value)}"'
        elif kind in (
            "invocation",
            "parent-invocation",
            "child-invocation",
            "new-invocation",
        ):
            data_attr = f' data-invocation-id="{escape(value)}"'
        else:
            data_attr = ""
        return (
            f'<a href="{url}" class="log-entity-link"'
            f"{data_attr}"
            f' title="{kind}: {escape(value)}">{escape(token)}</a>'
        )
    return escape(token)


def _dim_timestamp(html: str) -> str:
    """Wrap the leading timestamp portion in a muted span."""
    return _TS_RE.sub(r'<span class="log-ts">\1</span>', html)


# ── Jinja2 registration ───────────────────────────────────────────────────────
templates.env.globals["_render_log_html"] = render_log_html
