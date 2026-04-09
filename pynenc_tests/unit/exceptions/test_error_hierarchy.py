"""Validate exception inheritance chains for pynenc exceptions."""

import pytest

from pynenc.exceptions import (
    AlreadyInitializedError,
    ConcurrencyRetryError,
    ConfigError,
    ConfigMultiInheritanceError,
    InvalidTaskOptionsError,
    InvocationConcurrencyWithDifferentArgumentsError,
    InvocationError,
    InvocationNotFoundError,
    InvocationStatusError,
    InvocationStatusOwnershipError,
    InvocationStatusRaceConditionError,
    InvocationStatusTransitionError,
    PynencError,
    RetryError,
    RunnerError,
    RunnerNotExecutableError,
    SerializationError,
    StateBackendError,
    TaskError,
    TaskParallelProcessingError,
    TaskRoutingError,
)

# ── pynenc hierarchy ─────────────────────────────────────────────────


@pytest.mark.parametrize(
    "exc_cls, parents",
    [
        # Retry branch
        (RetryError, [PynencError]),
        (ConcurrencyRetryError, [RetryError, PynencError]),
        # Serialization
        (SerializationError, [PynencError]),
        # Task branch
        (TaskError, [PynencError]),
        (InvalidTaskOptionsError, [TaskError, PynencError]),
        (TaskRoutingError, [TaskError, PynencError]),
        (
            InvocationConcurrencyWithDifferentArgumentsError,
            [TaskRoutingError, TaskError, PynencError],
        ),
        (TaskParallelProcessingError, [TaskError, PynencError]),
        # Invocation
        (InvocationError, [PynencError]),
        # StateBackend branch
        (StateBackendError, [PynencError]),
        (InvocationNotFoundError, [StateBackendError, PynencError]),
        # Runner
        (RunnerNotExecutableError, [PynencError]),
        (RunnerError, [PynencError]),
        # Config
        (ConfigError, [PynencError]),
        (ConfigMultiInheritanceError, [ConfigError, PynencError]),
        # Other
        (AlreadyInitializedError, [PynencError]),
        # Status branch
        (InvocationStatusError, [PynencError]),
        (InvocationStatusRaceConditionError, [InvocationStatusError, PynencError]),
        (InvocationStatusTransitionError, [InvocationStatusError, PynencError]),
        (InvocationStatusOwnershipError, [InvocationStatusError, PynencError]),
    ],
    ids=lambda x: x.__name__ if isinstance(x, type) else None,
)
def test_pynenc_exception_hierarchy(
    exc_cls: type, parents: list[type]
) -> None:
    """Every pynenc exception must be a subclass of its documented parents."""
    for parent in parents:
        assert issubclass(exc_cls, parent), (
            f"{exc_cls.__name__} is not a subclass of {parent.__name__}"
        )

