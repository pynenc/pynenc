"""Unit tests for pynmon.util.log_parser module.

Tests log line parsing, ANSI stripping, JSON log support, bracket context
extraction, entity reference parsing, timestamp handling, and multi-line splitting.

Key components:
- parse_log_line: Single line parser
- parse_log_lines: Multi-line splitter + parser
- timestamp_to_utc: UTC normalization
- _try_extract_json_text: JSON log line support
"""

import json
from datetime import UTC, datetime, timezone

import pytest

from pynmon.util.log_parser import (
    ParsedRunner,
    parse_log_line,
    parse_log_lines,
    timestamp_to_utc,
)

# ################################################################################### #
# PARSE_LOG_LINE — BASIC STRUCTURE
# ################################################################################### #


def test_parse_log_line_should_extract_full_context() -> None:
    """Full log line with runners, invocation, task, timestamp, and level."""
    line = (
        "2025-01-15 12:30:45.123+00:00 INFO  pynenc.task "
        "[MTR(32002c1a).TR(1829f97b)8f238bbb-0cba-45af-b56e-5a472e86ea97:module.func] "
        "Task started"
    )
    result = parse_log_line(line)

    assert result.is_valid
    assert result.level == "INFO"
    assert result.timestamp is not None
    assert result.timestamp.year == 2025
    assert len(result.runners) == 2
    assert result.runners[0] == ParsedRunner(cls_abbr="MTR", partial_id="32002c1a")
    assert result.runners[1] == ParsedRunner(cls_abbr="TR", partial_id="1829f97b")
    assert result.invocation_id == "8f238bbb-0cba-45af-b56e-5a472e86ea97"
    assert result.task_key == "module.func"
    assert result.raw_bracket is not None


def test_parse_log_line_should_handle_single_runner() -> None:
    """Log line with one runner and invocation only."""
    line = (
        "2025-01-15 12:30:45+00:00 WARNING pynenc.orch "
        "[TR(abc12345)11111111-2222-3333-4444-555555555555:pkg.task] warning msg"
    )
    result = parse_log_line(line)

    assert result.is_valid
    assert result.level == "WARNING"
    assert len(result.runners) == 1
    assert result.runners[0].cls_abbr == "TR"
    assert result.runners[0].partial_id == "abc12345"
    assert result.invocation_id == "11111111-2222-3333-4444-555555555555"
    assert result.task_key == "pkg.task"


def test_parse_log_line_should_handle_runner_only_bracket() -> None:
    """Bracket with runner only, no invocation."""
    line = "2025-01-15 12:30:45+00:00 DEBUG pynenc.run [MTR(32002c1a)] Starting runner"
    result = parse_log_line(line)

    assert result.is_valid
    assert len(result.runners) == 1
    assert result.invocation_id is None
    assert result.task_key is None


def test_parse_log_line_should_handle_invocation_only_bracket() -> None:
    """Bracket with UUID only (no runner context recorded)."""
    line = (
        "2025-01-15 12:30:45+00:00 INFO pynenc.run "
        "[aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee:my_module.my_func] doing work"
    )
    result = parse_log_line(line)

    assert result.is_valid
    assert result.runners == []
    assert result.invocation_id == "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    assert result.task_key == "my_module.my_func"


def test_parse_log_line_should_handle_no_bracket() -> None:
    """Plain log line without bracket context."""
    line = "2025-01-15 12:30:45+00:00 ERROR pynenc Some error occurred"
    result = parse_log_line(line)

    assert not result.is_valid
    assert result.level == "ERROR"
    assert result.timestamp is not None
    assert result.runners == []
    assert result.invocation_id is None


def test_parse_log_line_should_handle_empty_string() -> None:
    """Empty input produces an empty ParsedLogLine."""
    result = parse_log_line("")

    assert not result.is_valid
    assert result.raw == ""


def test_parse_log_line_should_handle_garbage_input() -> None:
    """Non-log text produces a non-valid result."""
    result = parse_log_line("this is not a log line at all")

    assert not result.is_valid
    assert result.timestamp is None
    assert result.level is None


# ################################################################################### #
# PARSE_LOG_LINE — ANSI COLOUR STRIPPING
# ################################################################################### #


def test_parse_log_line_should_strip_ansi_codes() -> None:
    """ANSI escape codes from colored terminal output are stripped before parsing."""
    ansi_line = (
        "\x1b[32m2025-01-15 12:30:45+00:00\x1b[0m \x1b[34mINFO\x1b[0m  pynenc.task "
        "\x1b[36m[TR(abc12345)11111111-2222-3333-4444-555555555555:mod.fn]\x1b[0m msg"
    )
    result = parse_log_line(ansi_line)

    assert result.is_valid
    assert result.level == "INFO"
    assert len(result.runners) == 1
    assert result.invocation_id == "11111111-2222-3333-4444-555555555555"
    assert result.task_key == "mod.fn"


# ################################################################################### #
# PARSE_LOG_LINE — JSON LOG SUPPORT
# ################################################################################### #


def test_parse_log_line_should_handle_json_log_line() -> None:
    """JSON log lines (from JsonFormatter) are parsed via their text field."""
    text_value = (
        "2025-01-15 12:30:45+00:00 INFO  pynenc.task "
        "[TR(abc12345)11111111-2222-3333-4444-555555555555:mod.fn] Task done"
    )
    json_line = json.dumps(
        {
            "severity": "INFO",
            "message": "Task done",
            "text": text_value,
            "logger": "pynenc.task",
        }
    )
    result = parse_log_line(json_line)

    assert result.is_valid
    assert result.level == "INFO"
    assert result.invocation_id == "11111111-2222-3333-4444-555555555555"
    assert result.task_key == "mod.fn"
    assert len(result.runners) == 1


def test_parse_log_line_should_ignore_json_without_text_field() -> None:
    """JSON without a text field falls back to normal parsing."""
    json_line = json.dumps({"severity": "INFO", "message": "no text field"})
    result = parse_log_line(json_line)

    # The JSON string itself won't match log format
    assert not result.is_valid


def test_parse_log_line_should_ignore_invalid_json() -> None:
    """Malformed JSON-looking input is treated as plain text."""
    result = parse_log_line("{this is not valid json}")

    assert not result.is_valid


def test_parse_log_line_should_ignore_non_json_starting_with_brace() -> None:
    """Input starting with { but not actually JSON is handled gracefully."""
    result = parse_log_line("{partial json without closing")

    assert not result.is_valid


# ################################################################################### #
# PARSE_LOG_LINE — TIMESTAMP EXTRACTION
# ################################################################################### #


def test_parse_log_line_should_extract_tz_aware_timestamp() -> None:
    """Timestamp with timezone offset parsed correctly."""
    line = "2025-06-20 14:05:30.456+02:00 INFO pynenc Something"
    result = parse_log_line(line)

    assert result.timestamp is not None
    assert result.timestamp.tzinfo is not None
    assert result.timestamp.hour == 14
    assert result.timestamp.year == 2025


def test_parse_log_line_should_extract_naive_timestamp() -> None:
    """Timestamp without timezone offset parsed as naive datetime."""
    line = "2025-06-20 14:05:30 INFO pynenc [TR(a1b2c3d4)] msg"
    result = parse_log_line(line)

    assert result.timestamp is not None
    assert result.timestamp.tzinfo is None
    assert result.timestamp.second == 30


def test_parse_log_line_should_extract_microsecond_timestamp() -> None:
    """Timestamp with microseconds (no timezone) parsed correctly."""
    line = "2025-06-20 14:05:30.123456 DEBUG pynenc [TR(aabbccdd)] msg"
    result = parse_log_line(line)

    assert result.timestamp is not None
    assert result.timestamp.microsecond == 123456


# ################################################################################### #
# PARSE_LOG_LINE — LOG LEVEL EXTRACTION
# ################################################################################### #


def test_parse_log_line_should_extract_all_levels() -> None:
    """All standard log levels are recognized."""
    for level in ("DEBUG", "INFO", "WARNING", "ERROR", "CRITICAL"):
        line = f"2025-01-01 00:00:00+00:00 {level} pynenc [TR(aa)] msg"
        result = parse_log_line(line)
        assert result.level == level, f"Failed for {level}"


# ################################################################################### #
# PARSE_LOG_LINE — ENTITY REFERENCE EXTRACTION
# ################################################################################### #


def test_parse_log_line_should_extract_single_entity_refs() -> None:
    """Single entity references in message body are extracted."""
    line = (
        "2025-01-15 12:30:45+00:00 INFO pynenc.orch "
        "Routing invocation:aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee "
        "to runner:my-runner-id"
    )
    result = parse_log_line(line)

    assert result.is_valid
    kinds = {ref.kind for ref in result.entity_refs}
    assert "invocation" in kinds
    assert "runner" in kinds


def test_parse_log_line_should_extract_task_entity_ref() -> None:
    """Task entity references in message body are extracted."""
    line = "2025-01-15 12:30:45+00:00 INFO pynenc Registered task:my_app.tasks.add"
    result = parse_log_line(line)

    assert any(
        ref.kind == "task" and ref.value == "my_app.tasks.add"
        for ref in result.entity_refs
    )


def test_parse_log_line_should_extract_list_entity_refs() -> None:
    """List-form entity references invocations:[id,id] are expanded."""
    uid1 = "11111111-1111-1111-1111-111111111111"
    uid2 = "22222222-2222-2222-2222-222222222222"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc Processing invocations:[{uid1},{uid2}]"
    )
    result = parse_log_line(line)

    inv_refs = [ref for ref in result.entity_refs if ref.kind == "invocation"]
    assert len(inv_refs) >= 2
    values = {ref.value for ref in inv_refs}
    assert uid1 in values
    assert uid2 in values


def test_parse_log_line_should_deduplicate_entity_refs() -> None:
    """Duplicate entity references are not repeated."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc invocation:{uid} same invocation:{uid}"
    )
    result = parse_log_line(line)

    inv_refs = [ref for ref in result.entity_refs if ref.kind == "invocation"]
    assert len(inv_refs) == 1


def test_parse_log_line_should_extract_extended_entity_kinds() -> None:
    """Extended entity kinds like parent-invocation, workflow are recognized."""
    uid = "aaaaaaaa-bbbb-cccc-dddd-eeeeeeeeeeee"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc parent-invocation:{uid} workflow:wf-123"
    )
    result = parse_log_line(line)

    kinds = {ref.kind for ref in result.entity_refs}
    assert "parent-invocation" in kinds
    assert "workflow" in kinds


# ################################################################################### #
# PARSE_LOG_LINES — MULTI-LINE SPLITTING
# ################################################################################### #


def test_parse_log_lines_should_split_multiple_entries() -> None:
    """Multiple log entries are split and parsed individually."""
    text = (
        "2025-01-15 12:30:45+00:00 INFO pynenc [TR(aa)] first msg\n"
        "2025-01-15 12:30:46+00:00 WARNING pynenc [TR(bb)] second msg\n"
        "2025-01-15 12:30:47+00:00 ERROR pynenc [TR(cc)] third msg\n"
    )
    results = parse_log_lines(text)

    assert len(results) == 3
    assert results[0].level == "INFO"
    assert results[1].level == "WARNING"
    assert results[2].level == "ERROR"


def test_parse_log_lines_should_handle_continuation_lines() -> None:
    """Continuation lines (e.g. stack traces) attach to preceding entry."""
    text = (
        "2025-01-15 12:30:45+00:00 ERROR pynenc [TR(aa)] Exception occurred\n"
        "Traceback (most recent call last):\n"
        '  File "task.py", line 10, in run\n'
        "    raise RuntimeError()\n"
        "RuntimeError\n"
        "2025-01-15 12:30:46+00:00 INFO pynenc [TR(bb)] Recovery\n"
    )
    results = parse_log_lines(text)

    assert len(results) == 2
    assert results[0].level == "ERROR"
    assert results[1].level == "INFO"


def test_parse_log_lines_should_handle_empty_input() -> None:
    """Empty input returns empty list."""
    assert parse_log_lines("") == []
    assert parse_log_lines("   ") == []


def test_parse_log_lines_should_discard_leading_non_log_text() -> None:
    """Text before the first log entry is discarded."""
    text = (
        "Some banner text\n"
        "=================\n"
        "2025-01-15 12:30:45+00:00 INFO pynenc [TR(aa)] actual log\n"
    )
    results = parse_log_lines(text)

    assert len(results) == 1
    assert results[0].level == "INFO"


def test_parse_log_lines_should_handle_single_line() -> None:
    """Single log line is returned as one-element list."""
    text = "2025-01-15 12:30:45+00:00 INFO pynenc [TR(aa)] single line"
    results = parse_log_lines(text)

    assert len(results) == 1
    assert results[0].is_valid


# ################################################################################### #
# TIMESTAMP_TO_UTC
# ################################################################################### #


def test_timestamp_to_utc_should_convert_tz_aware() -> None:
    """TZ-aware timestamps are converted to UTC."""
    _ = timezone(
        offset=datetime.now(tz=UTC).utcoffset()
        or __import__("datetime").timedelta(hours=-5)
    )
    ts = datetime(
        2025,
        1,
        15,
        12,
        0,
        0,
        tzinfo=timezone(offset=__import__("datetime").timedelta(hours=-5)),
    )
    result = timestamp_to_utc(ts)

    assert result is not None
    assert result.tzinfo == UTC
    assert result.hour == 17  # 12:00 EST = 17:00 UTC


def test_timestamp_to_utc_should_preserve_naive() -> None:
    """Naive timestamps (no TZ) are returned unchanged."""
    ts = datetime(2025, 1, 15, 12, 0, 0)
    result = timestamp_to_utc(ts)

    assert result is not None
    assert result.tzinfo is None
    assert result.hour == 12


def test_timestamp_to_utc_should_handle_none() -> None:
    """None input returns None."""
    assert timestamp_to_utc(None) is None


def test_timestamp_to_utc_should_handle_utc_input() -> None:
    """Already-UTC timestamps are returned unchanged."""
    ts = datetime(2025, 1, 15, 12, 0, 0, tzinfo=UTC)
    result = timestamp_to_utc(ts)

    assert result is not None
    assert result.tzinfo == UTC
    assert result.hour == 12


# ################################################################################### #
# PARSE_LOG_LINE — TRIGGER ENTITY REFERENCES (Phase 1)
# ################################################################################### #


def test_parse_log_line_should_extract_event_ref() -> None:
    """``event:{id}`` tokens are recognised as entity refs."""
    eid = "11111111-2222-3333-4444-555555555555"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.event.emitted event:{eid}"
    )
    result = parse_log_line(line)
    assert any(r.kind == "event" and r.value == eid for r in result.entity_refs)


def test_parse_log_line_should_extract_trigger_run_ref() -> None:
    """``trigger-run:{id}`` tokens are recognised."""
    rid = "aaaaaaaa-1111-2222-3333-cccccccccccc"
    line = f"2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.run.executed trigger-run:{rid}"
    result = parse_log_line(line)
    assert any(r.kind == "trigger-run" and r.value == rid for r in result.entity_refs)


def test_parse_log_line_should_extract_condition_and_trigger_refs() -> None:
    """``condition:{id}``, ``valid-condition:{id}`` and ``trigger:{id}`` tokens parse."""
    line = (
        "2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.condition.matched "
        "condition:cond-abc valid-condition:vc-1 trigger:trig-xyz"
    )
    result = parse_log_line(line)
    kinds = {(r.kind, r.value) for r in result.entity_refs}
    assert ("condition", "cond-abc") in kinds
    assert ("valid-condition", "vc-1") in kinds
    assert ("trigger", "trig-xyz") in kinds


def test_parse_log_line_should_keep_full_hash_condition_ids() -> None:
    """Status/result condition IDs use # separators and must not be truncated."""
    condition_id = (
        "condition#events_monitoring.test_on_result_args.score_case#success#"
        "static_b0cf4e73b135df10f7bedc1fdf57e52b485605335f7e8f7059714c7c03d5f906"
        "_result_callable_events_monitoring.test_on_result_args._is_high_score"
    )
    valid_condition_id = f"valid_condition_{condition_id}_context_result-src"
    line = (
        "2026-05-24 13:16:34+00:00 INFO pynenc.trig "
        f"trigger.condition.matched condition:{condition_id} "
        f"valid-condition:{valid_condition_id} context:ResultContext"
    )

    result = parse_log_line(line)
    refs = {(r.kind, r.value) for r in result.entity_refs}

    assert ("condition", condition_id) in refs
    assert ("condition", "condition") not in refs
    assert ("valid-condition", valid_condition_id) in refs
    assert ("valid-condition", "valid_condition_condition") not in refs


def test_parse_log_line_should_keep_full_cron_condition_ids() -> None:
    """Cron condition refs include spaces but should not degrade to cron_."""
    condition_id = "cron_*/15 * * * *"
    line = (
        "2026-05-24 13:16:34+00:00 INFO pynenc.trig "
        f"trigger.condition.matched condition:{condition_id} context:CronContext"
    )

    result = parse_log_line(line)
    refs = {(r.kind, r.value) for r in result.entity_refs}

    assert ("condition", condition_id) in refs
    assert ("condition", "cron_") not in refs


def test_parse_log_line_should_keep_full_valid_condition_with_iso_context() -> None:
    """Valid-condition IDs containing ISO timestamps must not be truncated."""
    valid_id = "valid_condition_cond-cron_context_cron_2026-05-18T11:03:06.831000+00:00"
    line = (
        "2026-05-18 11:03:06.831000+00:00 INFO pynenc.trig "
        "trigger.condition.matched condition:cond-cron "
        f"valid-condition:{valid_id} context:CronContext "
        "cron:2026-05-18T11:03:06.831000+00:00 "
        "timestamp:2026-05-18T11:03:06.831000+00:00"
    )
    result = parse_log_line(line)
    kinds = {(r.kind, r.value) for r in result.entity_refs}

    assert ("valid-condition", valid_id) in kinds
    assert ("context", "CronContext") in kinds
    assert ("cron", "2026-05-18T11:03:06.831000+00:00") in kinds


@pytest.mark.parametrize(
    ("context_name", "source_kind", "extra_tokens"),
    [
        ("EventContext", "event", "event:evt-1 code:result.any.captured"),
        (
            "StatusContext",
            "source-invocation",
            "source-invocation:src-1 status:SUCCESS task:pkg.task",
        ),
        (
            "ResultContext",
            "source-invocation",
            "source-invocation:src-1 status:SUCCESS task:pkg.task",
        ),
        (
            "ExceptionContext",
            "source-invocation",
            "source-invocation:src-1 status:FAILED exception:ValueError task:pkg.task",
        ),
        (
            "CronContext",
            "cron",
            "cron:2026-05-18T11:03:06.831000+00:00 timestamp:2026-05-18T11:03:06.831000+00:00",
        ),
    ],
)
def test_parse_log_line_should_parse_condition_matched_for_all_contexts(
    context_name: str,
    source_kind: str,
    extra_tokens: str,
) -> None:
    """Condition matched logs remain parseable for every trigger context type."""
    line = (
        "2026-05-18 11:03:06.831000+00:00 INFO pynenc.trig "
        "trigger.condition.matched condition:cond-1 valid-condition:valid-1 "
        f"context:{context_name} {extra_tokens}"
    )

    result = parse_log_line(line)
    refs = {(r.kind, r.value) for r in result.entity_refs}

    assert ("condition", "cond-1") in refs
    assert ("valid-condition", "valid-1") in refs
    assert ("context", context_name) in refs
    assert any(kind == source_kind for kind, _ in refs)


def test_parse_log_line_should_extract_source_and_triggered_invocation_refs() -> None:
    """source-invocation and triggered-invocation are parsed as their own kinds."""
    src = "11111111-1111-1111-1111-111111111111"
    new = "22222222-2222-2222-2222-222222222222"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.run.executed "
        f"source-invocation:{src} triggered-invocation:{new}"
    )
    result = parse_log_line(line)
    kinds = {(r.kind, r.value) for r in result.entity_refs}
    assert ("source-invocation", src) in kinds
    assert ("triggered-invocation", new) in kinds


def test_parse_log_line_should_expand_trigger_list_refs() -> None:
    """Plural trigger-run/event list forms expand into singular refs."""
    eid1 = "aaaa1111-aaaa-aaaa-aaaa-aaaaaaaaaaaa"
    eid2 = "bbbb2222-bbbb-bbbb-bbbb-bbbbbbbbbbbb"
    rid = "cccc3333-cccc-cccc-cccc-cccccccccccc"
    line = (
        f"2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.run.executed "
        f"events:[{eid1},{eid2}] trigger-runs:[{rid}]"
    )
    result = parse_log_line(line)
    events = {r.value for r in result.entity_refs if r.kind == "event"}
    runs = {r.value for r in result.entity_refs if r.kind == "trigger-run"}
    assert events == {eid1, eid2}
    assert runs == {rid}


def test_parse_log_line_should_expand_source_and_triggered_invocation_list_refs() -> (
    None
):
    """Plural ``source-invocations:[...]`` keeps the role distinct from plain invocations."""
    sid = "ddddeeee-1111-2222-3333-ffffffffffff"
    tid = "eeeeffff-1111-2222-3333-aaaaaaaaaaaa"
    line = (
        "2025-01-15 12:30:45+00:00 INFO pynenc.trig trigger.run.executed "
        f"source-invocations:[{sid}] triggered-invocations:[{tid}]"
    )
    result = parse_log_line(line)
    sources = {r.value for r in result.entity_refs if r.kind == "source-invocation"}
    triggered = {
        r.value for r in result.entity_refs if r.kind == "triggered-invocation"
    }
    assert sid in sources
    assert tid in triggered
