"""
Stable log message vocabulary for the trigger system.

Centralizes the message names, entity-ref formatting, and participant
summaries that :class:`pynenc.trigger.base_trigger.BaseTrigger` emits during
event/condition/run lifecycle. Keeping the contract in one place lets the
Pynmon log-parser, renderer, docs, and tests share the same source of truth.

Message names use a ``trigger.<topic>.<verb>`` prefix so log filters can
slice the trigger lifecycle by topic without parsing message text.

Entity refs follow the ``kind:value`` convention already used by
``pynenc.util.log`` and ``pynmon.util.log_parser``. New kinds added here:

- ``event:{event_id}``
- ``trigger:{trigger_id}``
- ``trigger-run:{trigger_run_id}``
- ``condition:{condition_id}``
- ``valid-condition:{valid_condition_id}``
- ``source-invocation:{invocation_id}``
- ``triggered-invocation:{invocation_id}``
- ``atomic-service-run:{atomic_service_run_id}``
- ``cron:{iso_timestamp}``

List forms (``events:[...]`` etc.) follow the same convention.
"""

from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from pynenc.trigger.conditions import ConditionContext


# --------------------------------------------------------------------------- #
# Stable message names
# --------------------------------------------------------------------------- #


class TriggerLogMsg:
    """Stable lifecycle message names emitted by the trigger component."""

    EVENT_EMITTED = "trigger.event.emitted"
    CONDITION_MATCHED = "trigger.condition.matched"
    CONDITION_REGISTERED = "trigger.condition.registered"
    RUN_CLAIMED = "trigger.run.claimed"
    RUN_SKIPPED = "trigger.run.skipped"
    RUN_EXECUTED = "trigger.run.executed"
    RUN_STORED = "trigger.run.stored"
    CRON_CLAIMED = "trigger.cron.claimed"
    CRON_SKIPPED = "trigger.cron.skipped"


# --------------------------------------------------------------------------- #
# Entity ref kinds (mirror pynmon.util.log_parser)
# --------------------------------------------------------------------------- #


#: New singular ref kinds introduced for trigger observability.
TRIGGER_ENTITY_REF_KINDS: tuple[str, ...] = (
    "event",
    "trigger",
    "trigger-run",
    "condition",
    "valid-condition",
    "source-invocation",
    "triggered-invocation",
    "atomic-service-run",
    "cron",
)

#: New plural list-form ref kinds.
TRIGGER_ENTITY_LIST_KINDS: tuple[str, ...] = (
    "events",
    "triggers",
    "trigger-runs",
    "conditions",
    "valid-conditions",
    "source-invocations",
    "triggered-invocations",
)


# --------------------------------------------------------------------------- #
# Formatting helpers
# --------------------------------------------------------------------------- #


def ref(kind: str, value: str | None) -> str:
    """Format a single ``kind:value`` entity ref, returning '' for empty values.

    :param kind: Entity kind (e.g. ``"event"``, ``"trigger-run"``).
    :param value: Entity id; when falsy the helper returns the empty string so
        callers can drop the token from the message with a simple ``filter``.
    """
    if not value:
        return ""
    return f"{kind}:{value}"


def ref_list(kind_plural: str, values: list[str]) -> str:
    """Format a list-form entity ref like ``events:[id1,id2]``.

    Returns '' when ``values`` is empty so callers can omit the token cleanly.
    """
    if not values:
        return ""
    return f"{kind_plural}:[{','.join(values)}]"


def context_source_ref(context: "ConditionContext") -> str:
    """Return the appropriate entity ref for the source of a condition context.

    Maps each :class:`ConditionContext` subclass to the most informative ref:

    - ``EventContext`` -> ``event:{event_id}``
    - ``StatusContext`` / ``ResultContext`` / ``ExceptionContext`` ->
      ``source-invocation:{invocation_id}``
    - ``CronContext`` -> ``"cron:{iso_timestamp}"`` (no entity id; the
      timestamp is the only meaningful source identifier).

    Returns the empty string when no source can be identified, so callers may
    join the result into a message without an extra branch.
    """
    from pynenc.trigger.conditions import (
        CronContext,
        EventContext,
        StatusContext,
    )

    if isinstance(context, EventContext):
        return ref("event", context.event_id)
    # ExceptionContext and ResultContext are StatusContext subclasses; the
    # source-invocation ref is the same for all three.
    if isinstance(context, StatusContext):
        return ref("source-invocation", str(context.invocation_id))
    if isinstance(context, CronContext):
        return f"cron:{context.timestamp.isoformat()}"
    return ""


def context_extra_tokens(context: "ConditionContext") -> list[str]:
    """Return extra ``key:value`` tokens describing a context for log messages.

    These tokens travel alongside the source ref so the Log Explorer can show
    the matched status/exception/event-code without an extra backend hop. They
    intentionally use plain ``key:value`` text rather than entity refs because
    they do not point at hydratable monitoring records.
    """
    from pynenc.trigger.conditions import (
        CronContext,
        EventContext,
        ExceptionContext,
        StatusContext,
    )

    tokens: list[str] = []
    if isinstance(context, EventContext):
        if context.event_code:
            tokens.append(f"code:{context.event_code}")
        return tokens
    # ExceptionContext first because it is a StatusContext subclass and adds
    # the exception_type token on top of status/task.
    if isinstance(context, ExceptionContext):
        if context.status is not None:
            tokens.append(f"status:{context.status.name}")
        if context.exception_type:
            tokens.append(f"exception:{context.exception_type}")
        tokens.append(f"task:{context.call_id.task_id}")
        return tokens
    if isinstance(context, StatusContext):
        if context.status is not None:
            tokens.append(f"status:{context.status.name}")
        tokens.append(f"task:{context.call_id.task_id}")
        return tokens
    if isinstance(context, CronContext):
        tokens.append(f"timestamp:{context.timestamp.isoformat()}")
    return tokens


def join_tokens(*tokens: str) -> str:
    """Join non-empty tokens with single spaces.

    Convenience wrapper so callers can compose log messages from a list of
    optional refs without manual ``filter`` boilerplate.
    """
    return " ".join(t for t in tokens if t)
