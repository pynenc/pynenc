"""Log line parser for the pynmon Log Explorer.

Parses structured log lines emitted by pynenc.util.log.ColoredFormatter
and extracts the runner context chain, invocation ID, task key, timestamp,
and structured entity references from the message body.

Supports multi-line input: ``parse_log_lines`` splits a block of text into
individual log entries (each starting with a timestamp + level) and returns
a ``ParsedLogLine`` for every entry.

Log format (compact context mode, typical output):
    TIMESTAMP LEVEL  LOGGER [CLS(id).CLS(id)INV_ID:module.func] message

Examples of the context bracket:
    [MTR(32002c1a).TR(1829f97b)8f238bbb-0cba-45af-b56e-5a472e86ea97:module.func]
    [TR(abc12345)uuid:module.func]
    [uuid:module.func]            # no runner context recorded
    [MTR(32002c1a)]               # runner only, no invocation

Structured entity references in message body (convention):
    invocation:UUID  runner:ID  worker:ID  task:KEY
    invocations:[UUID,UUID,...]  parent-invocation:UUID  child-invocation:UUID
    current-owner-runner:UUID  attempted-owner-runner:UUID
    workflow:UUID  sub-workflow:UUID  parent-workflow:UUID
"""

import json
import re
from dataclasses import dataclass, field
from datetime import UTC, datetime

# ── regex ──────────────────────────────────────────────────────────────────────
_ANSI_RE = re.compile(r"\x1b\[[0-9;]*m")
_BRACKET_RE = re.compile(r"\[([^\]]+)\]")
_RUNNER_RE = re.compile(r"([A-Za-z]+)\(([^)]+)\)")
_UUID_RE = re.compile(
    r"[0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}",
    re.IGNORECASE,
)
_TIMESTAMP_RE = re.compile(
    r"(\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?)"
)

# Matches the start of a new log entry: date + optional TZ + level keyword
_LOG_LINE_START_RE = re.compile(
    r"^\d{4}-\d{2}-\d{2}\s+\d{2}:\d{2}:\d{2}(?:\.\d+)?(?:[+-]\d{2}:\d{2})?\s+"
    r"(?:DEBUG|INFO|WARNING|ERROR|CRITICAL)\s",
    re.MULTILINE,
)

# Structured references: key:value where value is a UUID or similar ID
_ENTITY_REF_RE = re.compile(
    r"(?:invocation|runner|worker|task"
    r"|parent-invocation|child-invocation|new-invocation"
    r"|current-owner-runner|attempted-owner-runner"
    r"|workflow|sub-workflow|parent-workflow)"
    r":([0-9a-f]{8}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{4}-[0-9a-f]{12}"
    r"|[0-9a-zA-Z._-]+)",
    re.IGNORECASE,
)
# List form: invocations:[id,id,...] / workflows:[id,id,...]
_ENTITY_LIST_RE = re.compile(
    r"(invocations|runners|workers|workflows):\[([^\]]*)\]",
    re.IGNORECASE,
)

# Known log-level keywords for colouring
_LOG_LEVELS = {"DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"}


# ── data model ─────────────────────────────────────────────────────────────────
@dataclass
class ParsedRunner:
    """One runner entry extracted from the log context chain.

    :param str cls_abbr: Compact class abbreviation, e.g. 'MTR' or 'TR'
    :param str partial_id: Truncated runner ID as it appeared in the log
    """

    cls_abbr: str
    partial_id: str


@dataclass
class EntityRef:
    """A structured entity reference extracted from the log message body.

    :param str kind: Reference type — 'invocation', 'runner', 'worker', 'task', etc.
    :param str value: The entity ID or key
    """

    kind: str
    value: str


@dataclass
class ParsedLogLine:
    """Structured result of parsing a single pynenc log line.

    :param str raw: The original raw log line text
    :param list[ParsedRunner] runners: Ordered runner chain (parent first)
    :param str | None invocation_id: Full UUID invocation ID from context bracket
    :param str | None task_key: Task module.func key from context bracket
    :param str | None raw_bracket: Raw content of the context bracket
    :param datetime | None timestamp: Parsed timestamp from the log line
    :param str | None level: Log level (INFO, WARNING, ERROR, …)
    :param list[EntityRef] entity_refs: Structured entity references from the message
    :param bool is_valid: True when at least one component was extracted
    """

    raw: str = ""
    runners: list[ParsedRunner] = field(default_factory=list)
    invocation_id: str | None = None
    task_key: str | None = None
    raw_bracket: str | None = None
    timestamp: datetime | None = None
    level: str | None = None
    entity_refs: list[EntityRef] = field(default_factory=list)
    is_valid: bool = False


# ── public API ─────────────────────────────────────────────────────────────────
def parse_log_lines(text: str) -> list[ParsedLogLine]:
    """Split a block of text into individual log entries and parse each one.

    Each log entry starts with a timestamp and log level.  Lines that do not
    match a new entry are appended to the previous one (e.g. multi-line stack
    traces or diagnostic blocks).  Non-log cruft before the first entry is
    discarded.

    :param str text: Raw log text, possibly multiple lines
    :return: Ordered list of parsed log lines
    """
    entries = _split_log_entries(text)
    return [parse_log_line(entry) for entry in entries]


def parse_log_line(log_line: str) -> ParsedLogLine:
    """Parse a pynenc log line and extract its context components.

    Strips ANSI escape codes before parsing so coloured terminal output
    can be pasted directly.  Also handles JSON log lines (from JsonFormatter)
    by extracting the ``text`` field and parsing it as a standard text line.
    Extracts structured entity references like ``invocation:UUID`` from the
    message body.

    :param str log_line: Raw log line (may contain ANSI colour codes or JSON)
    :return: ParsedLogLine with runners, invocation_id, task_key, timestamp,
             and entity_refs populated
    """
    raw = log_line.strip()
    # Handle JSON log lines by extracting the human-readable text field
    if json_text := _try_extract_json_text(raw):
        raw = json_text
        clean = json_text
    else:
        clean = _strip_ansi(raw)
    timestamp = _extract_timestamp(clean)
    level = _extract_level(clean)
    bracket = _extract_bracket(clean)
    entity_refs = _extract_entity_refs(clean)

    if bracket is None and not entity_refs:
        return ParsedLogLine(raw=raw, timestamp=timestamp, level=level)

    parsed = _parse_bracket(bracket) if bracket else ParsedLogLine()
    parsed.raw = raw
    parsed.timestamp = timestamp
    parsed.level = level
    parsed.entity_refs = entity_refs
    parsed.is_valid = parsed.is_valid or bool(entity_refs)
    return parsed


# ── splitting helpers ──────────────────────────────────────────────────────────
def _split_log_entries(text: str) -> list[str]:
    """Split multi-line text into individual log entries.

    An entry starts at a line matching the timestamp + level pattern.
    Continuation lines are attached to the preceding entry.
    """
    clean = _strip_ansi(text)
    starts: list[int] = [m.start() for m in _LOG_LINE_START_RE.finditer(clean)]
    if not starts:
        return [ln for ln in text.strip().splitlines() if ln.strip()]

    entries: list[str] = []
    for i, start in enumerate(starts):
        end = starts[i + 1] if i + 1 < len(starts) else len(clean)
        entries.append(clean[start:end].strip())
    return entries


# ── parsing helpers ────────────────────────────────────────────────────────────
def _try_extract_json_text(line: str) -> str | None:
    """Try to parse a JSON log line and extract its text field.

    When pynenc is configured with JSON log format (JsonFormatter), each line
    is a JSON object containing a ``text`` field with the human-readable log
    representation that pynmon can parse normally.

    :param str line: Raw log line that might be JSON
    :return: The text field value if JSON with text key, None otherwise
    """
    stripped = line.strip()
    if not stripped.startswith("{"):
        return None
    try:
        obj = json.loads(stripped)
        return obj.get("text")
    except (json.JSONDecodeError, TypeError):
        return None


def _strip_ansi(text: str) -> str:
    return _ANSI_RE.sub("", text)


def _extract_timestamp(text: str) -> datetime | None:
    """Extract the first ISO-ish timestamp, with optional TZ offset."""
    if m := _TIMESTAMP_RE.search(text):
        raw = m.group(1)
        for fmt in (
            "%Y-%m-%d %H:%M:%S.%f%z",
            "%Y-%m-%d %H:%M:%S%z",
            "%Y-%m-%d %H:%M:%S.%f",
            "%Y-%m-%d %H:%M:%S",
        ):
            try:
                return datetime.strptime(raw, fmt)
            except ValueError:
                continue
    return None


def timestamp_to_utc(ts: datetime | None) -> datetime | None:
    """Convert a parsed timestamp to UTC for state-backend queries.

    TZ-aware timestamps are converted; naive ones are assumed local and
    left unchanged (best-effort for legacy logs without offset).

    :param datetime | None ts: Parsed timestamp (may be tz-aware or naive)
    :return: UTC datetime or None
    """
    if ts is None:
        return None
    if ts.tzinfo is not None:
        return ts.astimezone(UTC)
    return ts


def _extract_level(text: str) -> str | None:
    """Extract the log level from the beginning of the line."""
    for lvl in _LOG_LEVELS:
        if re.search(rf"\b{lvl}\b", text):
            return lvl
    return None


def _extract_bracket(text: str) -> str | None:
    m = _BRACKET_RE.search(text)
    return m.group(1) if m else None


def _parse_bracket(bracket: str) -> ParsedLogLine:
    runners = [ParsedRunner(cls, pid) for cls, pid in _RUNNER_RE.findall(bracket)]
    inv_match = _UUID_RE.search(bracket)
    inv_id = inv_match.group(0) if inv_match else None
    task_key = _extract_task_key(bracket, inv_id)
    return ParsedLogLine(
        runners=runners,
        invocation_id=inv_id,
        task_key=task_key,
        raw_bracket=bracket,
        is_valid=bool(runners or inv_id or task_key),
    )


def _extract_task_key(bracket: str, inv_id: str | None) -> str | None:
    """Extract task key (module.func) after the UUID or runner chain."""
    if inv_id:
        idx = bracket.lower().index(inv_id.lower()) + len(inv_id)
        after = bracket[idx:]
    else:
        runner_hits = list(_RUNNER_RE.finditer(bracket))
        after = bracket[runner_hits[-1].end() :] if runner_hits else bracket
    return after[1:].strip() or None if after.startswith(":") else None


def _extract_entity_refs(text: str) -> list[EntityRef]:
    """Extract structured entity references from the full log line.

    Finds single references like ``invocation:UUID`` and list references
    like ``invocations:[UUID,UUID]``.
    """
    refs: list[EntityRef] = []
    seen: set[tuple[str, str]] = set()

    # Single references: invocation:UUID, worker:ID, runner:ID, task:KEY
    for m in _ENTITY_REF_RE.finditer(text):
        full = m.group(0)
        kind, value = full.split(":", 1)
        key = (kind, value)
        if key not in seen:
            seen.add(key)
            refs.append(EntityRef(kind=kind, value=value))

    # List references: invocations:[id,id,...]
    for m in _ENTITY_LIST_RE.finditer(text):
        plural_kind = m.group(1).lower()
        singular = plural_kind.rstrip("s")
        for item in m.group(2).split(","):
            val = item.strip()
            if val:
                key = (singular, val)
                if key not in seen:
                    seen.add(key)
                    refs.append(EntityRef(kind=singular, value=val))

    return refs
