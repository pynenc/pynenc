from abc import ABC, abstractmethod
from functools import cached_property
import signal
import threading
import time
from typing import TYPE_CHECKING, Optional, Any
import warnings


from pynenc.exceptions import RunnerNotExecutableError
from ..conf.config_runner import ConfigRunner

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from types import FrameType


class BaseRunner(ABC):
    """
    The Runner will execute invocations from the broker.

      - It requires an app because needs to know about broker, orchestrator...
      - But the main reason, the runner will affect the behaviour of the task result, e.g.
        * if the running works with subprocesses may implement a pipe to communicate
          (so it can pause/resume processes)
        * if it's an async environment, the value should be an async function to wait
          for distributed results
        * if it's a cloud function and needs to be as fast as possible with only one thread
          then it will not want to wait more than 'x' and instead, create a 'callback' save status
          convert current execution in a task that will be call when the result is ready
        * if it's a multiprocessing runner in a kubernetes pod with capabilities to create new pods
          then it may ...
        * asyncio worker, it just runs several tasks in one processor, the value should wait with async

    """

    def __init__(self, app: "Pynenc") -> None:
        self.app = app
        self.app.runner = self
        self.running = False

    @cached_property
    def conf(self) -> ConfigRunner:
        return ConfigRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

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
                "Running in a secondary thread. Signal handling will be skipped."
            )
        self.running = True
        self.app.logger.info(f"Starting runner {self.__class__.__name__}...")
        self._on_start()

    @abstractmethod
    def _on_stop(self) -> None:
        """This method is called when the runner stops"""

    def on_stop(self) -> None:
        """This method is called when the runner stops"""
        self.running = False
        self.app.logger.info(f"Stopping runner {self.__class__.__name__}...")
        self._on_stop()

    @abstractmethod
    def runner_loop_iteration(self) -> None:
        """
        One iteration of the runner loop.
        Subclasses should implement this method to process invocations.
        """

    def stop_runner_loop(
        self, signum: Optional[int] = None, frame: Optional["FrameType"] = None
    ) -> None:
        """Stops the runner loop"""
        self.app.logger.info(
            f"Received signal {signum=} {frame=} Stopping runner loop..."
        )
        self.running = False

    @abstractmethod
    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocation: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """This method is called from the result method of an invocation
        It signals the runner that the running invocation is waiting for the result of the result invocation

        The running invocation may be None, when the result was called from outside a runner (e.g. user environment)
        In that case will be handle by the DummyRunner (default in the pynenc app to handle this cases)

        The runner has the oportunity to define the waiting behaviour of the running invocation in this method
        Otherwise the running invocation will infinetely loop until the result invocation is ready
        """

    def run(self) -> None:
        """Starts the runner"""
        self.on_start()
        try:
            while self.running:
                self.runner_loop_iteration()
        except KeyboardInterrupt:
            self.app.logger.warning("KeyboardInterrupt received. Stopping runner...")
        except Exception as e:
            self.app.logger.exception(f"Exception in runner loop: {e}")
            raise e
        finally:
            self.on_stop()


class DummyRunner(BaseRunner):
    """This runner is a placeholder for the Pynenc app.
    It will be used when the app is defined in any other python environment than a pynenc runner.
    e.g.
      - A script that defines the app, decorates some tasks and then route them and finish
        Such a script doesn't plan to run anything, just to trigger some tasks that will later run in actual runners

    """

    def _on_start(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def _on_stop(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def runner_loop_iteration(self) -> None:
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
        time.sleep(1)
