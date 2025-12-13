import asyncio
from datetime import UTC, datetime
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
from pynenc.exceptions import RunnerNotExecutableError, PynencError
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    from types import FrameType

    from pynenc.app import Pynenc


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

    @property
    @abstractmethod
    def cache(self) -> dict:
        """
        Returns the runner cache.

        :return: A dictionary representing the runner cache
        """

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
        self.running = True
        self.app.logger.info("Starting runner...")
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

    def stop_runner_loop(
        self, signum: int | None = None, frame: "FrameType | None" = None
    ) -> None:
        """
        Stops the runner loop, typically in response to a signal.

        :param int | None signum: Signal number
        :param FrameType | None frame: Frame object at the time the signal was received
        """
        self.app.logger.info(
            f"Received signal {signum=} {frame=} Stopping runner loop..."
        )
        self.running = False
        self._on_stop_runner_loop()

    @abstractmethod
    def _waiting_for_results(
        self,
        running_invocation_id: str,
        result_invocation_ids: list[str],
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
        running_invocation_id: str | None,
        result_invocation_ids: list[str],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the current thread and registers it to wait for the results of specified invocations.

        :param str | None running_invocation_id: The ID of the invocation that is waiting for results
        :param list[str] result_invocation_ids: A list of IDs of the invocations whose results are being awaited
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
        running_invocation_id: str | None,
        result_invocation_ids: list[str],
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

    def _check_atomic_services(self) -> None:
        """
        Check and run atomic global services if this runner is authorized.

        Executes trigger processing and invocation recovery in a single
        atomic window to prevent conflicts across distributed runners.
        Handles any PynencError exceptions to prevent service failures from
        stopping the runner loop.
        """
        current_time = time.time()
        check_interval_seconds = self.conf.atomic_service_check_interval_minutes * 60

        if current_time - self._last_atomic_service_check_time < check_interval_seconds:
            return

        self._last_atomic_service_check_time = current_time

        start_time = None
        try:
            self.app.orchestrator.register_runner_heartbeat(self.runner_context)
            if not self.app.orchestrator.should_run_atomic_service(self.runner_context):
                return
            start_time = datetime.now(UTC)
            self.app.logger.info(
                f"Runner {self.runner_id} executing atomic global services"
            )
            self.app.trigger.trigger_loop_iteration()
            self.app.orchestrator.invocation_recovery_service(self.runner_context)
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
                    self.runner_context, start_time, end_time
                )

    def run(self) -> None:
        """
        Starts the runner, initiating its main loop.

        Sets the current runner in the context so that any invocations
        registered from within running tasks will use this runner's context
        instead of falling back to ExternalRunner.
        """
        # Calls for initial setup on Runner implementations
        self.on_start()
        # Set it after on_start to ensure we get latest runner_id
        # Set this runner as the current runner in the context
        # This ensures tasks that register new invocations use this runner's context
        context.set_current_runner(self.app.app_id, self)
        try:
            while self.running:
                self._check_atomic_services()
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

    def _waiting_for_results(
        self,
        running_invocation_id: str,
        result_invocation_ids: list[str],
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
        runner_context = RunnerContext(
            runner_cls=self.__class__.__name__,
            runner_id=f"ExternalRunner@{socket.gethostname()}-{os.getpid()}",
        )
        super().__init__(app, runner_cache, runner_context)
