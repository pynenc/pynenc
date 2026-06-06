"""Unit tests for pynenc.trigger.log_messages helper.

Validates the entity-ref formatters and the context-to-ref/extra-tokens
mapping used by ``BaseTrigger`` to emit predefined trigger logs.
"""

from datetime import UTC, datetime

from pynenc.arguments import Arguments
from pynenc.identifiers.call_id import CallId
from pynenc.identifiers.invocation_id import InvocationId
from pynenc.identifiers.task_id import TaskId
from pynenc.invocation.status import InvocationStatus
from pynenc.trigger.conditions import (
    CronContext,
    EventContext,
    ExceptionContext,
    ResultContext,
    StatusContext,
)
from pynenc.trigger.log_messages import (
    TRIGGER_ENTITY_LIST_KINDS,
    TRIGGER_ENTITY_REF_KINDS,
    TriggerLogMsg,
    context_extra_tokens,
    context_source_ref,
    join_tokens,
    ref,
    ref_list,
)


def _make_call_id() -> CallId:
    return CallId(task_id=TaskId(module="pkg", func_name="task"), args_id="abcd")


def _status_ctx(status: InvocationStatus = InvocationStatus.SUCCESS) -> StatusContext:
    return StatusContext(
        call_id=_make_call_id(),
        invocation_id=InvocationId("inv-1"),
        arguments=Arguments(kwargs={}),
        status=status,
        disable_cache_args=(),
    )


def test_ref_should_format_kind_value_pair() -> None:
    assert ref("event", "evt-1") == "event:evt-1"


def test_ref_should_return_empty_for_falsy_value() -> None:
    assert ref("event", None) == ""
    assert ref("event", "") == ""


def test_ref_list_should_format_plural_token() -> None:
    assert ref_list("events", ["a", "b"]) == "events:[a,b]"


def test_ref_list_should_return_empty_for_empty_values() -> None:
    assert ref_list("events", []) == ""


def test_join_tokens_should_skip_empty_tokens() -> None:
    assert join_tokens("a", "", "b", "") == "a b"


def test_message_names_use_trigger_prefix() -> None:
    for attr in (
        TriggerLogMsg.EVENT_EMITTED,
        TriggerLogMsg.CONDITION_MATCHED,
        TriggerLogMsg.RUN_CLAIMED,
        TriggerLogMsg.RUN_EXECUTED,
        TriggerLogMsg.CRON_CLAIMED,
        TriggerLogMsg.CRON_SKIPPED,
    ):
        assert attr.startswith("trigger.")


def test_ref_kinds_match_parser_contract() -> None:
    """The kinds list documents what pynmon.util.log_parser must accept."""
    for k in (
        "event",
        "trigger",
        "trigger-run",
        "condition",
        "valid-condition",
        "source-invocation",
        "triggered-invocation",
        "atomic-service-run",
        "cron",
    ):
        assert k in TRIGGER_ENTITY_REF_KINDS
    for k in (
        "events",
        "triggers",
        "trigger-runs",
        "conditions",
        "valid-conditions",
        "source-invocations",
        "triggered-invocations",
    ):
        assert k in TRIGGER_ENTITY_LIST_KINDS


def test_context_source_ref_for_event_context() -> None:
    ctx = EventContext(event_code="x", payload={}, event_id="evt-1")
    assert context_source_ref(ctx) == "event:evt-1"


def test_context_source_ref_for_status_subclasses() -> None:
    """StatusContext and subclasses share the source-invocation source ref."""
    status_ctx = _status_ctx()
    res_ctx = ResultContext(
        call_id=_make_call_id(),
        invocation_id=InvocationId("inv-1"),
        arguments=Arguments(kwargs={}),
        status=InvocationStatus.SUCCESS,
        disable_cache_args=(),
        result=1,
    )
    exc_ctx = ExceptionContext(
        call_id=_make_call_id(),
        invocation_id=InvocationId("inv-1"),
        arguments=Arguments(kwargs={}),
        status=InvocationStatus.FAILED,
        disable_cache_args=(),
        exception_type="ValueError",
        exception_message="boom",
    )
    for c in (status_ctx, res_ctx, exc_ctx):
        assert context_source_ref(c) == "source-invocation:inv-1"


def test_context_source_ref_for_cron_context_uses_timestamp() -> None:
    ts = datetime(2025, 1, 1, 12, 0, tzinfo=UTC)
    ctx = CronContext(timestamp=ts)
    assert context_source_ref(ctx).startswith("cron:2025-01-01T12:00:00")


def test_context_extra_tokens_for_event() -> None:
    ctx = EventContext(event_code="order.created", payload={}, event_id="evt-1")
    assert context_extra_tokens(ctx) == ["code:order.created"]


def test_context_extra_tokens_for_status_includes_status_and_task() -> None:
    ctx = _status_ctx(InvocationStatus.SUCCESS)
    tokens = context_extra_tokens(ctx)
    assert "status:SUCCESS" in tokens
    assert any(t.startswith("task:") for t in tokens)


def test_context_extra_tokens_for_result_includes_status_and_task() -> None:
    ctx = ResultContext(
        call_id=_make_call_id(),
        invocation_id=InvocationId("inv-1"),
        arguments=Arguments(kwargs={}),
        status=InvocationStatus.SUCCESS,
        disable_cache_args=(),
        result={"ok": True},
    )

    tokens = context_extra_tokens(ctx)

    assert "status:SUCCESS" in tokens
    assert any(t.startswith("task:") for t in tokens)


def test_context_extra_tokens_for_exception_includes_exception_type() -> None:
    ctx = ExceptionContext(
        call_id=_make_call_id(),
        invocation_id=InvocationId("inv-1"),
        arguments=Arguments(kwargs={}),
        status=InvocationStatus.FAILED,
        disable_cache_args=(),
        exception_type="ValueError",
        exception_message="boom",
    )
    tokens = context_extra_tokens(ctx)
    assert "status:FAILED" in tokens
    assert "exception:ValueError" in tokens
    assert any(t.startswith("task:") for t in tokens)
