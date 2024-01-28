import multiprocessing
import threading
import time
from collections import defaultdict
from functools import cached_property
from typing import Any, NamedTuple, Optional

from pynenc.conf.config_runner import ConfigThreadRunner
from pynenc.invocation.dist_invocation import DistributedInvocation, InvocationStatus
from pynenc.runner.base_runner import BaseRunner


class ThreadInfo(NamedTuple):
    thread: threading.Thread
    invocation: DistributedInvocation


class ThreadRunner(BaseRunner):
    """
    ThreadRunner is a concrete implementation of BaseRunner that executes tasks in separate threads.

    It manages task invocations, handling their execution and lifecycle within individual threads.
    This runner is suitable for I/O-bound tasks and scenarios where shared memory between tasks is required.
    """

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
        """
        Indicates if the runner is compatible with in-memory components.
        :return: True, as each task is executed in a separate thread with shared memory.
        """
        return True

    @property
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.
        :return: An integer representing the maximum number of parallel tasks.
        """
        return max(self.conf.min_parallel_slots, self.max_threads)

    def _on_start(self) -> None:
        """
        Internal method called when the ThreadRunner starts.
        Initializes the data structures for managing invocations and threads.
        """
        # Initialize thread list and condition dictionary
        self.wait_conditions = defaultdict(threading.Condition)
        self.wait_invocation = defaultdict(set)
        self.threads = {}
        self.max_threads = multiprocessing.cpu_count()
        self.waiting_threads = 0

    def _on_stop(self) -> None:
        """
        Internal method called when the ThreadRunner stops.
        Joins all running threads and updates their invocation statuses.
        """
        # kill all the running threads and change invocation status to retry
        for thread_info in self.threads.values():
            thread_info.thread.join()
            self.app.orchestrator.set_invocation_status(
                thread_info.invocation, InvocationStatus.RETRY
            )

    def _on_stop_runner_loop(self) -> None:
        """
        Internal method called after receiving a signal to stop the runner loop.
        """
        # No specific actions needed for stopping the runner loop in ThreadRunner

    @property
    def available_threads(self) -> int:
        """
        Returns the number of available thread slots for new invocations.
        :return: An integer representing available thread slots.
        """
        self.threads = {k: v for k, v in self.threads.items() if v.thread.is_alive()}
        # do not consider waiting threads
        # in testing with only one thread available and only one worker
        # the waiting threads will prevent to run the invocation that will unlock
        # the invocations waiting in these threads
        return self.max_parallel_slots - len(self.threads)  # - self.waiting_threads

    def runner_loop_iteration(self) -> None:
        """
        Executes one iteration of the ThreadRunner loop.
        Handles the execution and monitoring of task invocations in separate threads.
        """
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
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the current thread and registers it to wait for the results of specified invocations.
        :param running_invocation: The invocation that is waiting for results.
        :param result_invocations: A list of invocations whose results are being awaited.
        :param runner_args: Additional arguments required for the ThreadRunner.
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
