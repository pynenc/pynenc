import multiprocessing
import threading
import time
from functools import cached_property
from typing import Any, NamedTuple

from pynenc.conf.config_runner import ConfigThreadRunner
from pynenc.invocation.dist_invocation import DistributedInvocation, InvocationStatus
from pynenc.exceptions import InvocationStatusError
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.runner_context import RunnerContext


class ThreadInfo(NamedTuple):
    thread: threading.Thread
    invocation: DistributedInvocation


class ThreadRunner(BaseRunner):
    """
    ThreadRunner is a concrete implementation of BaseRunner that executes tasks in separate threads.

    It manages task invocations, handling their execution and lifecycle within individual threads.
    This runner is suitable for I/O-bound tasks and scenarios where shared memory between tasks is required.
    """

    threads: dict[str, ThreadInfo]
    max_threads: int
    waiting_invocation_ids: set[str]

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
        self.threads = {}
        self.waiting_invocation_ids = set()
        self.max_threads = self.conf.max_threads or multiprocessing.cpu_count()

    def _on_stop(self) -> None:
        """
        Internal method called when the ThreadRunner stops.
        Joins all running threads and updates their invocation statuses.
        """
        runner_ctx = RunnerContext.from_runner(self)
        self.logger.debug("Stopping ThreadRunner, joining all threads.")
        # Join all running threads and mark their invocations for retry.
        for thread_info in self.threads.values():
            thread_info.thread.join()
            try:
                self.app.orchestrator.set_invocation_status(
                    thread_info.invocation.invocation_id, InvocationStatus.KILLED
                )
                self.app.orchestrator.reroute_invocations(
                    {thread_info.invocation},
                    runner_ctx,
                )
            except InvocationStatusError as e:
                self.logger.warning(
                    f"Not possible to set invocation {thread_info.invocation.invocation_id} to RETRY: {e}"
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
                self.waiting_invocation_ids.discard(
                    thread_info.invocation.invocation_id
                )
        self.threads = alive_threads
        # Only count threads not waiting
        running_threads = [
            k for k in self.threads if k not in self.waiting_invocation_ids
        ]
        return self.max_parallel_slots - len(running_threads)

    def runner_loop_iteration(self) -> None:
        """
        Executes one iteration of the ThreadRunner loop.
        Handles the execution and monitoring of task invocations in separate threads.
        """
        self.logger.debug(
            f"Starting runner loop iteration with {self.available_threads=}"
        )
        runner_ctx = RunnerContext.from_runner(self)
        invocations = self.app.orchestrator.get_invocations_to_run(
            self.available_threads, runner_ctx
        )

        for invocation in invocations:
            try:
                thread = threading.Thread(
                    target=invocation.run,
                    daemon=True,
                    args=[RunnerContext.from_runner(self)],
                )
                thread.start()
                self.threads[invocation.invocation_id] = ThreadInfo(thread, invocation)
                self.logger.debug(
                    f"Running invocation {invocation.invocation_id} on {thread=}"
                )
            except RuntimeError as e:
                self.logger.error(
                    f"Failed to start thread for {invocation.invocation_id}: {e}"
                )
                self.app.orchestrator.reroute_invocations({invocation}, runner_ctx)

        self.logger.debug(
            f"Finished loop iteration, sleeping for {self.conf.runner_loop_sleep_time_sec}s"
        )
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation_id: str,
        result_invocation_ids: list[str],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Handles invocations waiting for results by polling a local final cache instead of pausing threads.

        :param running_invocation: The invocation that is waiting for results.
        :param result_invocations: A list of invocations whose results are being awaited.
        :param runner_args: Additional arguments required for the ThreadRunner.
        """
        del runner_args
        self.waiting_invocation_ids.add(running_invocation_id)
