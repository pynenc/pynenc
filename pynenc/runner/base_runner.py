from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Type

from pynenc.exceptions import RunnerNotExecutableError

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result
    from types import TracebackType


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

    @abstractmethod
    def on_start(self) -> None:
        """This method is called when the runner starts"""

    @abstractmethod
    def on_stop(self) -> None:
        """This method is called when the runner stops"""

    @abstractmethod
    def start_runner_loop(self) -> None:
        """
        Starts the runner loop. Subclasses should implement this method to start the main
        runner loop that retrieves task invocations from the broker and executes them.
        """

    def run(self) -> None:
        """Starts the runner"""
        self.on_start()
        try:
            self.start_runner_loop()
        except KeyboardInterrupt:
            print("Shutting down gracefully...")
        finally:
            self.on_stop()


class DummyRunner(BaseRunner):
    """This runner is a placeholder for the Pynenc app.
    It will be used when the app is defined in any other python environment than a pynenc runner.
    e.g.
      - A script that defines the app, decorates some tasks and then route them and finish
        Such a script doesn't plan to run anything, just to trigger some tasks that will later run in actual runners

    """

    def on_start(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def on_stop(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )

    def start_runner_loop(self) -> None:
        raise RunnerNotExecutableError(
            "This runner is a placeholder for the Pynenc app"
        )
