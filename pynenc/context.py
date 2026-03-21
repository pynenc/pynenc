"""
Context management for Pynenc execution environments.

This module maintains context for invocations and execution within the Pynenc application.
It provides thread-local storage for runner contexts that can be hierarchically nested.

Key components:
- RunnerContext management (set/get/clear)
- Distributed invocation context tracking
- Automatic logging context integration
"""

import threading
from typing import TYPE_CHECKING, Any

from pynenc.runner.runner_context import RunnerContext
from pynenc.runner.base_runner import ExternalRunner

# Create a thread-local data storage
thread_local = threading.local()

if TYPE_CHECKING:
    from pynenc.invocation.conc_invocation import ConcurrentInvocation
    from pynenc.invocation.dist_invocation import DistributedInvocation
    from pynenc.runner.base_runner import BaseRunner
    from pynenc.app import Pynenc

# Sync invocation context uses thread-local storage for thread safety
# - It is a dictionary with the format {app_id: invocation}


def _get_sync_inv_context_storage() -> dict[str, "ConcurrentInvocation | None"]:
    """Get thread-local sync invocation context storage, creating if needed."""
    storage = getattr(thread_local, "sync_inv_context", None)
    if storage is None:
        storage = {}
        thread_local.sync_inv_context = storage
    return storage


# =============================================================================
# Storage Access Helpers
# =============================================================================


def _get_runner_context_storage() -> dict[str, RunnerContext]:
    """Get thread-local runner context storage, creating if needed."""
    storage = getattr(thread_local, "runner_context", None)
    if storage is None:
        storage = {}
        thread_local.runner_context = storage
    return storage


def _get_dist_inv_context_storage() -> dict[str, "DistributedInvocation | None"]:
    """Get thread-local distributed invocation context storage, creating if needed."""
    storage = getattr(thread_local, "dist_inv_context", None)
    if storage is None:
        storage = {}
        thread_local.dist_inv_context = storage
    return storage


def _get_app_storage() -> "Pynenc | None":
    """Get thread-local app storage."""
    return getattr(thread_local, "current_app", None)


# =============================================================================
# RunnerContext Management
# =============================================================================


def get_runner_context(app_id: str) -> RunnerContext | None:
    """
    Get the current runner context for the given app.

    :param str app_id: The app identifier.
    :return: The current runner context, or None if not set.
    """
    return _get_runner_context_storage().get(app_id)


def set_runner_context(app_id: str, runner_ctx: RunnerContext) -> None:
    """
    Set the runner context for the given app.

    :param str app_id: The app identifier.
    :param RunnerContext runner_ctx: The runner context to set.
    """
    _get_runner_context_storage()[app_id] = runner_ctx


def clear_runner_context(app_id: str) -> None:
    """
    Clear the runner context for the given app.

    :param str app_id: The app identifier.
    """
    storage = _get_runner_context_storage()
    if app_id in storage:
        del storage[app_id]


def get_or_create_runner_context(app_id: str) -> RunnerContext:
    """
    Get the current runner context, creating an ExternalRunner context if none exists.

    :param str app_id: The app identifier.
    :return: The current runner context (never None).
    """
    # First check for directly set RunnerContext
    if runner_ctx := get_runner_context(app_id):
        return runner_ctx

    # Then check for a runner instance (set by BaseRunner.run())
    if runner := get_current_runner(app_id):
        return RunnerContext.from_runner(runner)

    # No context set - we're in an external process
    # Create an ExternalRunner context with hostname-pid for stable identification
    return ExternalRunner.get_default_external_runner_context()


# Alias for backward compatibility
get_current_runner_context = get_or_create_runner_context

# =============================================================================
# Distributed Invocation Context
# =============================================================================


def get_dist_invocation_context(app_id: str) -> "DistributedInvocation | None":
    """
    Get the current distributed invocation context for the given app.

    :param str app_id: The app identifier.
    :return: The current invocation context for the given app.
    """
    return _get_dist_inv_context_storage().get(app_id)


def swap_dist_invocation_context(
    app_id: str, invocation: "DistributedInvocation | None"
) -> "DistributedInvocation | None":
    """
    Set the current invocation context for the given app and returns the previous.

    :param str app_id: The app identifier.
    :param DistributedInvocation invocation: The invocation to set as the current context.
    :return: The previous invocation context.
    """
    storage = _get_dist_inv_context_storage()
    previous_invocation = storage.get(app_id)
    storage[app_id] = invocation
    return previous_invocation


# =============================================================================
# App Context Management
# =============================================================================


def get_current_app() -> "Pynenc | None":
    """
    Get the current app from thread-local storage.

    :return: The current app instance, or None if not set.
    """
    return _get_app_storage()


def set_current_app(app: "Pynenc") -> None:
    """
    Set the current app in thread-local storage.

    :param Pynenc app: The app instance to set.
    """
    thread_local.current_app = app


# =============================================================================
# Runner Args (Legacy - kept for backward compatibility)
# =============================================================================


def get_runner_args() -> dict[str, Any] | None:
    """
    Get the runner arguments from thread-local storage.

    :return: The runner arguments for the current thread, or None if not set.
    """
    return getattr(thread_local, "runner_args", None)


def set_runner_args(args: dict[str, Any] | None) -> None:
    """
    Set the runner arguments in thread-local storage.

    :param dict[str, Any] | None args: The runner arguments to set.
    """
    thread_local.runner_args = args


# =============================================================================
# Runner Instance Management (for runners that need instance access)
# =============================================================================


def _get_runner_storage() -> dict[str, "BaseRunner"]:
    """Get thread-local runner storage, creating if needed."""
    storage = getattr(thread_local, "current_runner", None)
    if storage is None:
        storage = {}
        thread_local.current_runner = storage
    return storage


def get_current_runner(app_id: str) -> "BaseRunner | None":
    """
    Retrieve the current runner for the given app_id from thread-local storage.

    This function allows each thread or process to access its own runner instance,
    which is critical in multi-process environments like MultiThreadRunner where
    each process runs a ThreadRunner and needs to reference its own runner instance
    without conflicting with others.

    :param str app_id: The application identifier.
    :return: The current runner instance if set in the current thread/process, else None.
    """
    return _get_runner_storage().get(app_id)


def set_current_runner(app_id: str, runner: "BaseRunner") -> None:
    """
    Set the current runner for the given app_id in thread-local storage.

    This is used to associate a runner with the current thread or process context,
    enabling isolated execution environments.

    Also creates and sets the runner context from the runner.

    :param str app_id: The application identifier.
    :param BaseRunner runner: The runner instance to set.
    """
    _get_runner_storage()[app_id] = runner
    # Create and set runner context from runner
    runner_ctx = RunnerContext.from_runner(runner)
    set_runner_context(app_id, runner_ctx)


def clear_current_runner(app_id: str) -> None:
    """
    Clear the current runner and all associated contexts for the given app_id.

    Also clears the logging context.

    :param str app_id: The application identifier.
    """
    # Clear runner
    runner_storage = _get_runner_storage()
    if app_id in runner_storage:
        del runner_storage[app_id]

    # Clear runner context
    clear_runner_context(app_id)
