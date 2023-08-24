import multiprocessing
import threading
import time
from collections import defaultdict
from typing import TYPE_CHECKING, NamedTuple, Optional

from pynenc.invocation import DistributedInvocation, InvocationStatus

from .base_runner import BaseRunner

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result


class ThreadInfo(NamedTuple):
    thread: threading.Thread
    invocation: DistributedInvocation


class MemRunner(BaseRunner):
    wait_conditions: dict["DistributedInvocation", threading.Condition]
    wait_invocation: dict["DistributedInvocation", set["DistributedInvocation"]]
    threads: dict[str, ThreadInfo]
    max_threads: int
    waiting_threads: int

    def _on_start(self) -> None:
        # Initialize thread list and condition dictionary
        self.wait_conditions = defaultdict(threading.Condition)
        self.wait_invocation = defaultdict(set)
        self.threads = {}
        self.max_threads = multiprocessing.cpu_count()
        self.waiting_threads = 0

    def _on_stop(self) -> None:
        """kill all the running threads and change invocation status to retry"""
        for thread_info in self.threads.values():
            thread_info.thread.join()
            self.app.orchestrator.set_invocation_status(
                thread_info.invocation, InvocationStatus.RETRY
            )

    @property
    def available_threads(self) -> int:
        """Return the number of available threads"""
        self.threads = {k: v for k, v in self.threads.items() if v.thread.is_alive()}
        return self.max_threads - len(self.threads) - self.waiting_threads

    def runner_loop_iteration(self) -> None:
        for invocation in self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_threads
        ):
            thread = threading.Thread(target=invocation.run, daemon=True)
            thread.start()
            self.threads[invocation.invocation_id] = ThreadInfo(thread, invocation)
        for invocation in list(self.wait_conditions):
            if invocation.status.is_final():
                # Notify all waiting threads to continue
                with self.wait_conditions[invocation]:
                    self.wait_conditions[invocation].notify_all()
                self.wait_conditions.pop(invocation)
                # Set all waiting invocations status to running
                waiting_invocations = self.wait_invocation.pop(invocation)
                self.app.orchestrator.set_invocations_status(
                    list(waiting_invocations), InvocationStatus.RUNNING
                )
                self.waiting_threads -= len(waiting_invocations)
        time.sleep(1)

    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocations: list["DistributedInvocation"],
    ) -> None:
        """In this case we let the current thread waiting in a condition based in the result invocation
        So we ignore the running_invocation parameter
        """
        if not running_invocation:
            # running from outside this runner (user instantiate an app with this runner class,
            # but ask for an invocation result outside of the runner processes)
            time.sleep(1)
            return
        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.PAUSED
        )
        for result_invocation in result_invocations:
            self.wait_invocation[result_invocation].add(running_invocation)
            self.waiting_threads += 1
            with self.wait_conditions[result_invocation]:
                self.wait_conditions[result_invocation].wait()
