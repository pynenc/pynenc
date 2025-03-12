import multiprocessing
import threading
import time
from collections import OrderedDict
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

    wait_invocation: set["DistributedInvocation"]
    threads: dict[str, ThreadInfo]
    max_threads: int
    waiting_threads: int
    final_invocations: "OrderedDict[DistributedInvocation, None]"

    @cached_property
    def conf(self) -> ConfigThreadRunner:
        return ConfigThreadRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def cache(self) -> dict:
        """
        The cache for the ThreadRunner instance.
        :return: A dictionary representing the cache for the ThreadRunner.
        """
        if not self._runner_cache:
            self._runner_cache = {}
        return self._runner_cache

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
        return max(
            self.conf.min_parallel_slots, self.conf.min_threads, self.max_threads
        )

    def _on_start(self) -> None:
        """
        Internal method called when the ThreadRunner starts.
        Initializes the data infrastructures for managing invocations and threads.
        """
        self.wait_invocation = set()
        self.threads = {}
        self.max_threads = self.conf.max_threads or multiprocessing.cpu_count()
        self.waiting_threads = 0
        self.final_invocations = OrderedDict()

    def _on_stop(self) -> None:
        """
        Internal method called when the ThreadRunner stops.
        Joins all running threads and updates their invocation statuses.
        """
        # Join all running threads and mark their invocations for retry.
        for thread_info in self.threads.values():
            thread_info.thread.join()
            self.app.orchestrator.set_invocation_status(
                thread_info.invocation, InvocationStatus.RETRY
            )

    def _on_stop_runner_loop(self) -> None:
        """
        Internal method called after receiving a signal to stop the runner loop.
        """
        pass

    @property
    def available_threads(self) -> int:
        """
        Returns the number of available thread slots for new invocations.
        Joins finished threads so they no longer count against the limit.
        :return: An integer representing available thread slots.
        """
        # Rebuild the threads dictionary by joining finished threads.
        alive_threads = {}
        for k, thread_info in self.threads.items():
            if thread_info.thread.is_alive():
                alive_threads[k] = thread_info
            else:
                thread_info.thread.join()
        self.threads = alive_threads
        # Do not count waiting threads.
        return self.max_parallel_slots - len(self.threads)

    def runner_loop_iteration(self) -> None:
        """
        Executes one iteration of the ThreadRunner loop.
        Handles the execution and monitoring of task invocations in separate threads.
        """
        self.logger.debug(
            f"Starting runner loop iteration with {self.available_threads=}"
        )

        invocations = self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_threads
        )

        for invocation in invocations:
            try:
                self.app.logger.info(
                    f"{self.runner_id} starting invocation:{invocation.invocation_id}"
                )
                thread = threading.Thread(target=invocation.run, daemon=True)
                thread.start()
                self.threads[invocation.invocation_id] = ThreadInfo(thread, invocation)
                self.logger.info(f"Running {invocation=} on {thread=}")
            except RuntimeError as e:
                self.logger.error(
                    f"Failed to start thread for {invocation.invocation_id}: {e}"
                )
                self.app.orchestrator.reroute_invocations({invocation})

        # Check waiting conditions
        waiting_count = len(self.wait_invocation)
        self.logger.debug(f"Checking {waiting_count} waiting conditions")

        for invocation in list(self.wait_invocation):
            self.logger.debug(
                f"Checking invocation {invocation.invocation_id} status={invocation.status}"
            )
            if invocation.status.is_final():
                self.final_invocations[invocation] = None  # Add to ordered set
                self.wait_invocation.remove(invocation)
                self.logger.debug(f"{invocation=} on final {invocation.status=}")

        # Clean up final_invocations if over size limit
        while len(self.final_invocations) > self.conf.final_invocation_cache_size:
            old_inv, _ = self.final_invocations.popitem(last=False)  # Remove oldest
            self.logger.debug(f"Evicted old final invocation {old_inv.invocation_id}")

        self.logger.debug(
            f"Finished loop iteration, sleeping for {self.conf.runner_loop_sleep_time_sec}s"
        )
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation: "DistributedInvocation",
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Handles invocations waiting for results by polling a local final cache instead of pausing threads.

        :param running_invocation: The invocation that is waiting for results.
        :param result_invocations: A list of invocations whose results are being awaited.
        :param runner_args: Additional arguments required for the ThreadRunner.
        """
        del runner_args
        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.PAUSED
        )
        self.logger.debug(
            f"Pausing invocation {running_invocation.invocation_id} is waiting for others to finish"
        )

        # Register dependencies collectively
        self.wait_invocation.update(result_invocations)
        self.waiting_threads += 1

        # Poll final_invocations cache until all results are ready
        while not all(
            result_inv in self.final_invocations for result_inv in result_invocations
        ):
            self.logger.debug(
                f"Polling for {running_invocation.invocation_id} waiting on {len(result_invocations)} results"
            )
            time.sleep(self.conf.invocation_wait_results_sleep_time_sec)

        # All results are final, resume
        self.waiting_threads -= 1
        self.logger.debug(
            f"Resuming {running_invocation.invocation_id}, all results final"
        )
        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.RUNNING
        )
