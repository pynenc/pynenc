import os
import signal
import socket
import threading
import time
import warnings
from abc import ABC, abstractmethod
from functools import cached_property
from typing import TYPE_CHECKING, Any, Optional

from pynenc.conf.config_runner import ConfigRunner
from pynenc.exceptions import RunnerNotExecutableError
from pynenc.util.log import RunnerLogAdapter

if TYPE_CHECKING:
    from types import FrameType

    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation


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

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.app.runner = self
        self.running = False
        self.logger = RunnerLogAdapter(self.app.logger, self.runner_id)

    @cached_property
    def runner_id(self) -> str:
        """
        Unique identifier for the runner instance.
        :return: A string representing the unique identifier of the runner.
        """
        hostname = socket.gethostname()
        pid = os.getpid()
        return f"{self.__class__.__name__}({hostname}-{pid})"

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
        :return: True if compatible, False otherwise.
        """
        ...

    @property
    @abstractmethod
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.
        :return: An integer representing the maximum number of parallel tasks.
        """

    @abstractmethod
    def _on_start(self) -> None:
        """This method is called when the runner starts"""

    def on_start(self) -> None:
        """This method is called when the runner starts"""
        if threading.current_thread() is threading.main_thread():
            signal.signal(signal.SIGINT, self.stop_runner_loop)
            signal.signal(signal.SIGTERM, self.stop_runner_loop)
        else:
            warnings.warn(
                "Running in a secondary thread. Signal handling will be skipped.",
                stacklevel=2,
            )
        self.running = True
        self.logger.info("Starting runner...")
        self._on_start()

    @abstractmethod
    def _on_stop(self) -> None:
        """This method is called when the runner stops"""

    def on_stop(self) -> None:
        """This method is called when the runner stops"""
        self.running = False
        self.logger.info("Stopping runner...")
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
        self, signum: Optional[int] = None, frame: Optional["FrameType"] = None
    ) -> None:
        """
        Stops the runner loop, typically in response to a signal.
        :param signum: Signal number.
        :param frame: Frame object at the time the signal was received.
        """
        self.app.logger.info(
            f"Received signal {signum=} {frame=} Stopping runner loop..."
        )
        self.running = False
        self._on_stop_runner_loop()

    @abstractmethod
    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocation: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
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

        :param running_invocation: The invocation that is waiting for results.
        :param result_invocation: A list of invocations whose results are being awaited.
        :param runner_args: Additional arguments passed to the runner, specific to the runner's implementation.
        """

    def run(self) -> None:
        """Starts the runner, initiating its main loop."""
        self.on_start()
        try:
            while self.running:
                self.runner_loop_iteration()
        except KeyboardInterrupt:
            self.logger.warning("KeyboardInterrupt received. Stopping runner...")
        except Exception as e:
            self.logger.exception(f"Exception in runner loop: {e}")
            raise e
        finally:
            self.on_stop()


class DummyRunner(BaseRunner):
    """
    This runner is a placeholder for the Pynenc app.
    It will be used when the app is defined in any other Python environment than a Pynenc runner.

    Examples include:
      - A script that defines the app, decorates some tasks, routes them, and then finishes. Such a script does not plan to run anything itself but triggers tasks that will later run in actual runners.
    """

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

    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocation: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        del running_invocation, result_invocation, runner_args
        # invocation.result() was called from outside a runner
        # it will block and loop indefinetely until result is available
        time.sleep(self.conf.invocation_wait_results_sleep_time_sec)
