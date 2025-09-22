import os
import signal
from multiprocessing import Manager, Process, cpu_count
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

from pynenc.exceptions import RunnerError
from pynenc.invocation import InvocationStatus
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.runner_context import RunnerContext

if TYPE_CHECKING:
    pass


class ClassifiedInvocations(NamedTuple):
    """
    Categorized invocation IDs based on their status.

    :param final: List of invocation IDs that have reached their final status
    :param non_final: List of invocation IDs that are still in progress
    """

    final: list[str]
    non_final: list[str]


class ProcessRunner(BaseRunner):
    """
    ProcessRunner is a concrete implementation of BaseRunner that executes tasks in separate processes.

    It manages task invocations, handling their execution, monitoring, and lifecycle within individual processes.
    This runner is suitable for CPU-bound tasks and scenarios where task isolation is essential.
    """

    wait_invocation: dict[
        str, set[str]
    ]  # Maps invocation_id to set of waiting invocation_ids
    inv_id_to_processes: dict[str, Process]
    manager: Manager  # type: ignore
    runner_cache: dict

    max_processes: int

    @property
    def cache(self) -> dict:
        """Returns the cache for the ProcessRunner instance."""
        return self.runner_cache

    @staticmethod
    def mem_compatible() -> bool:
        """
        Indicates if the runner is compatible with in-memory components.
        :return: False, as each task is executed in a separate process with independent memory.
        """
        return False

    @property
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.
        :return: An integer representing the maximum number of parallel tasks, based on CPU count.
        """
        return max(self.conf.min_parallel_slots, self.max_processes)

    @property
    def runner_args(self) -> dict[str, Any]:
        """
        Provides arguments necessary for parent-subprocess communication in ProcessRunner.
        :return: A dictionary containing the 'wait_invocation' Managed dictionary for subprocesses.
        """
        # this is necessary for parent-subprocess communication on ProcessRunner
        # it passes the wait_invocation Managed dictinoary to the subprocesses
        # so they can notify the main loop when waiting for other invocatinos
        # the main loop will then pause the subprocesses
        return {"wait_invocation": self.wait_invocation}

    @property
    def waiting_processes(self) -> int:
        """
        Returns the number of processes that are currently waiting for other invocations to finish.
        :return: An integer representing the number of waiting processes.
        """
        if not self.wait_invocation:
            return 0
        return len(set.union(*self.wait_invocation.values()))

    def parse_args(self, args: dict[str, Any]) -> None:
        """
        Parses the arguments provided to the runner.
        :param args: A dictionary of arguments passed to the runner.
        """
        self.wait_invocation = args["wait_invocation"]

    def _on_start(self) -> None:
        """
        Internal method called when the ProcessRunner starts.
        Initializes the process manager and the data structures for managing invocations.
        """
        self.logger.info("Starting ProcessRunner")
        self.manager = Manager()
        self.wait_invocation = self.manager.dict()  # type: ignore
        self.runner_cache = self._runner_cache or self.manager.dict()  # type: ignore
        self.inv_id_to_processes = {}
        self.max_processes = cpu_count()

    def _on_stop(self) -> None:
        """
        Internal method called when the ProcessRunner stops.
        Terminates all running processes and updates their invocation statuses.
        """
        self.logger.info("Stopping ProcessRunner")
        for invocation_id, process in self.inv_id_to_processes.items():
            process.kill()
            self.app.orchestrator.set_invocation_status(
                invocation_id, InvocationStatus.RETRY
            )
            self.logger.info(f"Killing invocation {invocation_id}")
        self.manager.shutdown()  # type: ignore
        self.logger.info("ProcessRunner stopped")

    def _on_stop_runner_loop(self) -> None:
        """
        Internal method called after receiving a signal to stop the runner loop.
        Clears the wait_invocation dictionary.
        """
        self.logger.info("Stopping ProcessRunner loop")
        if hasattr(self, "wait_invocation") and self.wait_invocation is not None:
            self.wait_invocation.clear()
            self.wait_invocation = {}
        self.logger.info("ProcessRunner loop stopped")

    @property
    def available_processes(self) -> int:
        """
        Returns the number of available process slots for new invocations.
        :return: An integer representing available process slots.
        """
        for invocation_id in list(self.inv_id_to_processes):
            if not self.inv_id_to_processes[invocation_id].is_alive():
                self.inv_id_to_processes[invocation_id].join()
                del self.inv_id_to_processes[invocation_id]
        # discount waiting processes, they should do nothing
        # until the blocking invocation is finished
        # otherwise, running one worker with one process
        # will be lock indefintely until the blocking invocation runs
        return self.max_parallel_slots - len(self.inv_id_to_processes)

    def clasify_waiting_invocations(
        self,
    ) -> ClassifiedInvocations:
        """Will classify the waiting invocation IDs into final and non finals"""
        waiting_invocation_ids = list(self.wait_invocation.keys())
        if not waiting_invocation_ids:
            return ClassifiedInvocations([], [])
        final_invocation_ids = self.app.orchestrator.filter_final(
            waiting_invocation_ids
        )
        non_final_invocation_ids = [
            inv_id
            for inv_id in waiting_invocation_ids
            if inv_id not in final_invocation_ids
        ]
        return ClassifiedInvocations(final_invocation_ids, non_final_invocation_ids)

    def handle_waiting_invocations(self) -> None:
        """Handle the waiting invocations"""
        classified = self.clasify_waiting_invocations()
        # Pause processes waiting for non-final invocations
        for invocation_id in classified.non_final:
            for waiting_invocation_id in self.wait_invocation.get(invocation_id, []):
                if waiting_process := self.inv_id_to_processes.get(
                    waiting_invocation_id
                ):
                    if waiting_process.pid:
                        os.kill(waiting_process.pid, signal.SIGSTOP)
                        self.logger.info(
                            f"{waiting_invocation_id=} waiting for {invocation_id=}, pausing process {waiting_process.pid}"
                        )
        # Get the invocations that are waiting in finalized ones
        to_resume_invocation_ids: set[str] = set()
        for invocation_id in classified.final:
            if waiting_invocation_ids := self.wait_invocation.get(invocation_id, set()):
                to_resume_invocation_ids.update(waiting_invocation_ids)
                self.wait_invocation[invocation_id] = set()
                self.logger.info(f"{invocation_id=} finalized, resuming waiting ones")
        # Resume the processes waiting for finalized invocations
        # and set their status to RUNNING
        if to_resume_invocation_ids:
            for waiting_invocation_id in to_resume_invocation_ids:
                # Find the process for this waiting invocation ID
                if waiting_process := self.inv_id_to_processes.get(
                    waiting_invocation_id
                ):
                    if waiting_process.pid:
                        os.kill(waiting_process.pid, signal.SIGCONT)
                        self.logger.info(
                            f"{waiting_invocation_id=} resuming process {waiting_process.pid}"
                        )
            self.app.orchestrator.set_invocations_status(
                list(to_resume_invocation_ids), InvocationStatus.RUNNING
            )

    def runner_loop_iteration(self) -> None:
        """
        Executes one iteration of the ProcessRunner loop.
        Handles the execution and monitoring of task invocations in separate processes.
        """
        # called from parent process memory space
        self.logger.debug(f"starting runner loop iteration {self.available_processes=}")
        for invocation in self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_processes
        ):
            invocation.app.runner = self
            process = Process(
                target=invocation.run,
                kwargs={
                    "runner_args": self.runner_args,
                    "runner_ctx": RunnerContext.from_runner(self),
                },
                daemon=True,
            )
            self.app.logger.info(
                f"{self.runner_id} starting invocation:{invocation.invocation_id}"
            )
            process.start()
            self.logger.debug(
                f"Running invocation {invocation.invocation_id} on {process.pid=}"
            )
            if process.pid:
                self.inv_id_to_processes[invocation.invocation_id] = process
            else:
                # Optionally, raise an exception or log error if process.pid is not available.
                raise RunnerError("Failed to start process: PID not available")
        self.handle_waiting_invocations()

    def _waiting_for_results(
        self,
        running_invocation_id: str,
        result_invocation_ids: list[str],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the running process and registers it to wait for the results of specified invocations.
        :param running_invocation_id: The ID of the invocation that is waiting for results.
        :param result_invocation_ids: A list of IDs of invocations whose results are being awaited.
        :param runner_args: Additional arguments required for the ProcessRunner.
        """
        self.app.orchestrator.set_invocation_status(
            running_invocation_id, InvocationStatus.PAUSED
        )
        if not result_invocation_ids:
            return
        if not runner_args:
            raise RunnerError("runner_args should be defined for ProcessRunner")
        self.parse_args(runner_args)
        for result_inv_id in result_invocation_ids:
            current_waiters = set(self.wait_invocation.get(result_inv_id, set()))
            current_waiters.add(running_invocation_id)
            self.wait_invocation[result_inv_id] = current_waiters
            self.logger.debug(
                f"Invocation {running_invocation_id} is waiting for invocation {result_inv_id} to finish"
            )
