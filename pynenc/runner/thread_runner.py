import multiprocessing
import threading
import time
from collections import defaultdict
from functools import cached_property
from typing import Any, NamedTuple, Optional

from pynenc.invocation import DistributedInvocation, InvocationStatus

from ..conf.config_runner import ConfigThreadRunner
from .base_runner import BaseRunner


class ThreadInfo(NamedTuple):
    thread: threading.Thread
    invocation: DistributedInvocation


class ThreadRunner(BaseRunner):
    wait_conditions: dict["DistributedInvocation", threading.Condition]
    wait_invocation: dict["DistributedInvocation", set["DistributedInvocation"]]
    threads: dict[str, ThreadInfo]
    max_threads: int
    waiting_threads: int

    @cached_property
    def conf(self) -> ConfigThreadRunner:
        return ConfigThreadRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @staticmethod
    def mem_compatible() -> bool:
        # each task is executed in a different thread with shared memory
        return True

    @property
    def max_parallel_slots(self) -> int:
        return max(self.conf.min_parallel_slots, self.max_threads)

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

    def _on_stop_runner_loop(self) -> None:
        pass

    @property
    def available_threads(self) -> int:
        """Return the number of available threads"""
        self.threads = {k: v for k, v in self.threads.items() if v.thread.is_alive()}
        # do not consider waiting threads
        # in testing with only one thread available and only one worker
        # the waiting threads will prevent to run the invocation that will unlock
        # the invocations waiting in these threads
        return self.max_parallel_slots - len(self.threads)  # - self.waiting_threads

    def runner_loop_iteration(self) -> None:
        for invocation in self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_threads
        ):
            thread = threading.Thread(target=invocation.run, daemon=True)
            thread.start()
            self.threads[invocation.invocation_id] = ThreadInfo(thread, invocation)
            self.logger.info(f"Running {invocation=} on {thread=}")
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
                self.logger.debug(
                    f"{invocation=} on final {invocation.status=}, resuming {waiting_invocations=}"
                )
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """In this case we let the current thread waiting in a condition based in the result invocation
        So we ignore the running_invocation parameter
        """
        del runner_args
        if not running_invocation:
            # running from outside this runner (user instantiate an app with this runner class,
            # but ask for an invocation result outside of the runner processes)
            self.logger.debug(
                f"Waiting for {result_invocations=} from outside this runner"
            )
            time.sleep(self.conf.invocation_wait_results_sleep_time_sec)
            return
        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.PAUSED
        )
        self.logger.debug(
            f"Pausing invocation {running_invocation.invocation_id} is waiting for others to finish"
        )
        for result_invocation in result_invocations:
            self.wait_invocation[result_invocation].add(running_invocation)
            self.waiting_threads += 1
            with self.wait_conditions[result_invocation]:
                self.logger.debug(
                    f"Invocation {running_invocation.invocation_id} is waiting for invocation {result_invocation.invocation_id} to finish"
                )
                self.wait_conditions[result_invocation].wait()
