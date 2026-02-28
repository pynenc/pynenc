import asyncio
from datetime import UTC, datetime
import importlib
from logging import Logger
import os
import signal
import socket
import threading
import time
import warnings
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any

from pynenc import context
from pynenc.conf.config_runner import ConfigRunner
from pynenc.exceptions import (
    InvocationStatusError,
    InvocationStatusTransitionError,
    RunnerNotExecutableError,
    PynencError,
)
from pynenc.invocation.status import InvocationStatus
from pynenc.runner.runner_context import RunnerContext
from pynenc.runner.shutdown_diagnostics import classify_signal, log_runner_shutdown

if TYPE_CHECKING:
    from types import FrameType

    from pynenc.app import Pynenc
    from pynenc.identifiers.invocation_id import InvocationId


class BaseRunner(ABC):
    """
    The BaseRunner class defines the interface for a runner that executes task invocations.

    It interacts with various components of the Pynenc system, like the broker and orchestrator,
    and is responsible for handling the execution and life cycle of task invocations.

    - The runner's behavior can vary depending on the execution environment (e.g., subprocess, async, cloud function, multiprocessing).
    - It is designed to be subclassed for specific execution environments.
    """

    """
    The Runner will execute invocations from the broker.

    - It requires an app because it needs to know about the broker, orchestrator, etc.
    - The runner will affect the behavior of the task result, for example:
      * In a subprocess environment, it may implement a pipe to communicate for pausing/resuming processes.
      * In an async environment, the value should be an async function to wait for distributed results.
      * In a cloud function environment aiming for speed with a single thread, it might not wait more than 'x' seconds and instead, create a 'callback', save the status, and convert the current execution into a task that will be called when the result is ready.
      * In a multiprocessing environment in a Kubernetes pod with capabilities to create new pods, it may have different behaviors.
      * For an asyncio worker, it runs several tasks in one processor, and the value should wait with async.
    """

    def __init__(
        self,
        app: "Pynenc",
        runner_cache: dict | None = None,
        runner_context: RunnerContext | None = None,
    ) -> None:
        # Initialize instance attributes first, before making runner accessible
        self.running = False
        self._shutdown_signum: int | None = None
        self._runner_cache = runner_cache
        self._runner_context = runner_context or RunnerContext(self.__class__.__name__)

        # Set app relationship last, after all attributes are initialized
        self.app = app
        self.app.runner = self

        self._last_atomic_service_check_time = 0.0

    @property
    def logger(self) -> Logger:
        return self.app.logger

    @property
    def runner_id(self) -> str:
        """Unique identifier for the runner instance."""
        return self._runner_context.runner_id

    @property
    def runner_context(self) -> RunnerContext:
        """The RunnerContext associated with this runner."""
        return self._runner_context

    @cached_property
    def conf(self) -> ConfigRunner:
        return ConfigRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @staticmethod
    @abstractmethod
    def mem_compatible() -> bool:
        """
        Indicates if the runner is compatible with in-memory components.

        ```{important}
            In memory components can only be used for testing purposes in shared memory space.
        ```

        :return: True if compatible, False otherwise
        """
        ...

    @property
    @abstractmethod
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.

        :return: An integer representing the maximum number of parallel tasks
        """

    @abstractmethod
    def get_active_child_runner_ids(self) -> list[str]:
        """
        Returns the list of currently active child runner IDs.

        This method enables parent-based health reporting for runners that spawn
        child processes or workers. The parent can report which child runner_ids
        are still alive based on OS-level process checks (e.g., Process.is_alive()).

        The orchestrator uses this to register heartbeats for active children,
        avoiding false recovery of invocations owned by children that are still
        running.

        Runners that spawn child processes must return the runner_ids of their
        alive children. Runners that don't spawn children must return an empty list.

        :return: List of runner_ids for currently active child workers.
        """

    @abstractmethod
    def _on_start(self) -> None:
        """This method is called when the runner starts"""

    def on_start(self) -> None:
        """This method is called when the runner starts"""
        self.app.logger.info(f"Starting runner {self.runner_id}")
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.stop_runner_loop)
            signal.signal(signal.SIGTERM, self.stop_runner_loop)
        else:
            warnings.warn(
                "Running in a secondary thread. Signal handling will be skipped.",
                stacklevel=2,
            )
        self.init_trigger_tasks_modules()
        self.running = True
        self._on_start()

    @abstractmethod
    def _on_stop(self) -> None:
        """This method is called when the runner stops"""

    def on_stop(self) -> None:
        """This method is called when the runner stops"""
        self.app.logger.info(f"Stopping runner {self.runner_id}")
        self.running = False
        self.app.logger.info("Stopping runner...")
        self._on_stop()

    @abstractmethod
    def runner_loop_iteration(self) -> None:
        """
        One iteration of the runner loop.
        Subclasses should implement this method to process invocations.
        """

    @abstractmethod
    def _on_stop_runner_loop(self) -> None:
        """This method is called after the runner loop signal is received"""

    def _kill_and_reroute(
        self, invocation_id: Any, runner_ctx: "RunnerContext | None" = None
    ) -> None:
        """
        Mark an invocation as KILLED and reroute it for retry.

        Silently ignores if the invocation already reached a final status
        (it completed before we killed it — nothing to reroute).

        :param Any invocation_id: Invocation to kill and reroute
        :param RunnerContext | None runner_ctx: Runner context to use for ownership;
            defaults to self.runner_context. Pass the child's context when the parent
            is acting on behalf of a child (e.g. ProcessRunner killing a worker).
        """
        ctx = runner_ctx or self.runner_context
        try:
            self.app.orchestrator.set_invocation_status(
                invocation_id, InvocationStatus.KILLED, ctx
            )
            self.app.orchestrator.reroute_invocations({invocation_id}, ctx)
            self.logger.info(f"Rerouted invocation {invocation_id} after kill")
        except InvocationStatusTransitionError as e:
            if e.from_status and e.from_status.is_final():
                self.logger.debug(
                    f"Invocation {invocation_id} already in final status, no rerouting needed"
                )
            else:
                self.logger.warning(
                    f"Could not reroute invocation {invocation_id}: {e}"
                )
        except InvocationStatusError as e:
            self.logger.warning(
                f"Not possible to set invocation {invocation_id} to KILLED and reroute: {e}"
            )

    def _log_shutdown(self, signum: int | None) -> None:
        """
        Log diagnostics when the runner receives a shutdown signal.

        Override in subclasses to include active processes/threads/invocations.
        The default logs only the runner identity and system environment.
        """
        log_runner_shutdown(
            self.app.logger, self.__class__.__name__, self.runner_id, signum
        )

    def stop_runner_loop(
        self, signum: int | None = None, frame: "FrameType | None" = None
    ) -> None:
        """
        Stops the runner loop, typically in response to a signal.

        Logs shutdown diagnostics (including system env, running workers, and
        invocation state) before stopping. Critical for debugging OOM events.

        :param int | None signum: Signal number
        :param FrameType | None frame: Frame object at the time the signal was received
        """
        self._shutdown_signum = signum
        reason = classify_signal(signum)
        self.app.logger.warning(
            f"Received signal {signum=} reason={reason} Stopping runner loop..."
        )
        try:
            self._log_shutdown(signum)
        except Exception as e:
            self.app.logger.error(
                f"Failed to collect shutdown diagnostics: {e}", exc_info=True
            )
        self.running = False
        self._on_stop_runner_loop()

    @abstractmethod
    def _waiting_for_results(
        self,
        running_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Method called when an invocation is waiting for results from other invocations.

        ```{note}
            This method is called from the result method of an invocation
        ```

        The runner has the oportunity to define the waiting behaviour of the running invocation in this method
        Otherwise the running invocation will infinetely loop until the result invocation is ready

        ```{note}
            The running invocation may be None, when the result was called from outside a runner (e.g. user environment)
            In that case will be handle by the DummyRunner (default in the pynenc app to handle this cases)
        ```

        Subclasses can define the waiting behavior of the running invocation in this method.

        :param str running_invocation_id: The ID of the invocation that is waiting for results
        :param list[str] result_invocation_ids: A list of IDs of the invocations whose results are being awaited
        :param dict[str, Any] | None runner_args: Additional arguments passed to the runner, specific to the runner's implementation
        """

    def waiting_for_results(
        self,
        running_invocation_id: "InvocationId | None",
        result_invocation_ids: list["InvocationId"],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the current thread and registers it to wait for the results of specified invocations.

        :param InvocationId | None running_invocation_id: The ID of the invocation that is waiting for results
        :param list[InvocationId] result_invocation_ids: A list of IDs of the invocations whose results are being awaited
        :param dict[str, Any] | None runner_args: Additional arguments required for the ThreadRunner
        """
        if not running_invocation_id:
            # running from outside this runner (user instantiate an app with this runner class,
            # but ask for an invocation result outside of the runner processes)
            self.app.logger.debug(
                f"Waiting for {result_invocation_ids=} from outside this runner"
            )
            time.sleep(self.conf.invocation_wait_results_sleep_time_sec)
            return
        self._waiting_for_results(
            running_invocation_id, result_invocation_ids, runner_args
        )

    async def async_waiting_for_results(
        self,
        running_invocation_id: "InvocationId | None",
        result_invocation_ids: list["InvocationId"],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        if not running_invocation_id:
            # running from outside this runner (user instantiate an app with this runner class,
            # but ask for an invocation result outside of the runner processes)
            self.logger.debug(
                f"Async Waiting for {result_invocation_ids=} from outside this runner"
            )
            await asyncio.sleep(self.conf.invocation_wait_results_sleep_time_sec)
            return
        self._waiting_for_results(
            running_invocation_id, result_invocation_ids, runner_args
        )

    def init_trigger_tasks_modules(self) -> None:
        """Initialize trigger task modules and register trigger tasks."""
        modules = getattr(self.app.conf, "trigger_task_modules", None) or ()
        if modules:
            self.app.logger.info("Initializing trigger task modules:")
        for module_name in modules:
            msg = f"trigger module '{module_name}'"
            try:
                importlib.import_module(module_name)
                self.app.logger.info(f"  - Successfully imported {msg}")
            except ImportError as e:
                self.app.logger.warning(f"  - Error importing {msg}: {e}")
        self.app.register_core_tasks()
        self.app.register_deferred_triggers()

    def _check_atomic_services(self) -> None:
        """
        Check and run atomic global services if this runner is authorized.

        Executes trigger processing and invocation recovery in a single
        atomic window to prevent conflicts across distributed runners.
        Handles any PynencError exceptions to prevent service failures from
        stopping the runner loop.
        """
        current_time = time.time()
        check_interval_seconds = (
            self.app.conf.atomic_service_check_interval_minutes * 60
        )

        if current_time - self._last_atomic_service_check_time < check_interval_seconds:
            return

        self._last_atomic_service_check_time = current_time

        start_time = None
        try:
            if not self.app.orchestrator.should_run_atomic_service(self.runner_context):
                return
            start_time = datetime.now(UTC)
            self.app.logger.info(
                f"Runner {self.runner_id} executing atomic global services"
            )
            self.app.trigger.trigger_loop_iteration()
        except PynencError as e:
            self.app.logger.error(
                f"Error during atomic service execution: {e}", exc_info=True
            )
        except Exception as e:
            self.app.logger.exception(
                f"Unexpected error during atomic service execution: {e}"
            )
            raise
        finally:
            if start_time is not None:
                end_time = datetime.now(UTC)
                self.app.orchestrator.record_atomic_service_execution(
                    self.runner_context.runner_id, start_time, end_time
                )

    def run(self) -> None:
        """
        Starts the runner, initiating its main loop.

        Sets the current runner in the context so that any invocations
        registered from within running tasks will use this runner's context
        instead of falling back to ExternalRunner.
        """
        # Set this runner as the current runner in the context FIRST
        # This ensures all logs during startup show the correct runner context
        context.set_current_runner(self.app.app_id, self)
        # Calls for initial setup on Runner implementations
        self.on_start()
        try:
            while self.running:
                # We must know which worker are alive and running invocations
                # Otherwise, the running invocation recovery service may
                # incorrectly assume that invocations owned by dead workers
                # need to be recovered, when in fact their workers are still alive.
                self._report_child_runner_heartbeats()
                # Atomic services runs in different runners at different times
                # We use to minimize conflicts with triggering and recovery services
                self._check_atomic_services()
                # Main runner loop iteration
                self.runner_loop_iteration()
                time.sleep(self.conf.runner_loop_sleep_time_sec)
        except KeyboardInterrupt:
            self.app.logger.warning("KeyboardInterrupt received. Stopping runner...")
        except Exception as e:
            self.app.logger.exception(f"Exception in runner loop: {e}")
            raise e
        finally:
            self.on_stop()
            context.clear_current_runner(self.app.app_id)

    def _register_new_child_runner_context(self, child_context: RunnerContext) -> None:
        """
        Register a new child runner context and ensures that we have an initial heartbeat.

        :param RunnerContext child_context: The context of the child runner to register
        """
        self.app.state_backend.store_runner_context(child_context)
        self.app.orchestrator.register_runner_heartbeats([child_context.runner_id])

    def _report_child_runner_heartbeats(self) -> None:
        """
        Report heartbeats for active child runners to the orchestrator.

        This enables parent-based health reporting where the parent runner
        reports which child runner_ids are still alive based on OS-level
        process checks. This provides an additional liveness signal beyond
        the child's own heartbeat thread.
        """
        if active_child_ids := self.get_active_child_runner_ids():
            self.app.orchestrator.register_runner_heartbeats(active_child_ids)


class DummyRunner(BaseRunner):
    """
    This runner is a placeholder for the Pynenc app.
    It will be used when the app is defined in any other Python environment than a Pynenc runner.

    Examples include:
      - A script that defines the app, decorates some tasks, routes them, and then finishes. Such a script does not plan to run anything itself but triggers tasks that will later run in actual runners.
    """

    @property
    def cache(self) -> dict:
        if self._runner_cache is None:
            self._runner_cache = {}
        return self._runner_cache

    @staticmethod
    def mem_compatible() -> bool:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def _on_start(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def _on_stop(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def _on_stop_runner_loop(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def runner_loop_iteration(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    @property
    def max_parallel_slots(self) -> int:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def get_active_child_runner_ids(self) -> list[str]:
        """DummyRunner has no child runners."""
        return []

    def _waiting_for_results(
        self,
        running_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        # Parameters are ids (string) to conform with BaseRunner interface.
        del running_invocation_id, result_invocation_ids, runner_args
        # invocation.result() was called from outside a runner; sleep briefly
        time.sleep(self.conf.invocation_wait_results_sleep_time_sec)


class ExternalRunner(DummyRunner):
    """
    Represents an external/client context outside Pynenc runners.

    This runner captures hostname and PID information from the external process
    (e.g., user script, CLI) that registers invocations but doesn't execute them.
    It extends DummyRunner since it cannot execute tasks, but provides valid
    RunnerContext for tracking purposes.

    Uses hostname-pid as runner_id since external processes are not managed
    by Pynenc and we cannot guarantee UUID persistence across calls.
    """

    def __init__(
        self,
        app: "Pynenc",
        runner_cache: dict | None = None,
    ) -> None:
        runner_context = self.get_default_external_runner_context()
        super().__init__(app, runner_cache, runner_context)

    @classmethod
    def get_default_external_runner_context(cls) -> RunnerContext:
        """
        Create a RunnerContext for the current external process.

        :return: RunnerContext with hostname-pid as runner_id
        """
        return RunnerContext(
            runner_cls=cls.__name__,
            runner_id=f"ExternalRunner@{socket.gethostname()}-{os.getpid()}",
        )
