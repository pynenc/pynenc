import os
import signal
import time
from multiprocessing import Manager, Process, cpu_count
from typing import TYPE_CHECKING, Any, Optional

from pynenc.exceptions import RunnerError
from pynenc.invocation import InvocationStatus
from pynenc.runner.base_runner import BaseRunner

if TYPE_CHECKING:
    from pynenc.invocation.dist_invocation import DistributedInvocation


class ProcessRunner(BaseRunner):
    """
    ProcessRunner is a concrete implementation of BaseRunner that executes tasks in separate processes.

    It manages task invocations, handling their execution, monitoring, and lifecycle within individual processes.
    This runner is suitable for CPU-bound tasks and scenarios where task isolation is essential.
    """

    wait_invocation: dict["DistributedInvocation", set["DistributedInvocation"]]
    processes: dict["DistributedInvocation", Process]
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
        self.processes = {}
        self.max_processes = cpu_count()

    def _on_stop(self) -> None:
        """
        Internal method called when the ProcessRunner stops.
        Terminates all running processes and updates their invocation statuses.
        """
        self.logger.info("Stopping ProcessRunner")
        for invocation, process in self.processes.items():
            process.kill()
            self.app.orchestrator.set_invocation_status(
                invocation, InvocationStatus.RETRY
            )
            self.logger.info(f"Killing invocation {invocation.invocation_id}")
        self.manager.shutdown()  # type: ignore
        self.logger.info("ProcessRunner stopped")

    def _on_stop_runner_loop(self) -> None:
        """
        Internal method called after receiving a signal to stop the runner loop.
        Clears the wait_invocation dictionary.
        """
        self.logger.info("Stopping ProcessRunner loop")
        self.wait_invocation.clear()
        self.wait_invocation = {}
        self.logger.info("ProcessRunner loop stopped")

    @property
    def available_processes(self) -> int:
        """
        Returns the number of available process slots for new invocations.
        :return: An integer representing available process slots.
        """
        for invocation in list(self.processes.keys()):
            if not self.processes[invocation].is_alive():
                self.processes[invocation].join()
                del self.processes[invocation]
        # discount waiting processes, they should do nothing
        # until the blocking invocation is finished
        # otherwise, running one worker with one process
        # will be lock indefintely until the blocking invocation runs
        return self.max_parallel_slots - len(self.processes)  # - self.waiting_processes

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
                kwargs={"runner_args": self.runner_args},
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
                self.processes[invocation] = process
            else:
                # Optionally, raise an exception or log error if process.pid is not available.
                raise RunnerError("Failed to start process: PID not available")
        self.logger.debug("runner loop - check waiting invocations pending results")
        for invocation in list(self.wait_invocation.keys()):
            is_final = invocation.status.is_final()
            for waiting_invocation in self.wait_invocation.get(invocation, []):
                pid = self.processes.get(waiting_invocation)
                if pid and pid.pid:
                    if is_final:
                        os.kill(pid.pid, signal.SIGCONT)
                    else:
                        os.kill(pid.pid, signal.SIGSTOP)
            if is_final:
                waiting_invocations = self.wait_invocation.pop(invocation, set())
                self.logger.info(
                    f"{invocation=} on final {invocation.status=}, resuming {waiting_invocations=}"
                )
                self.app.orchestrator.set_invocations_status(
                    list(waiting_invocations), InvocationStatus.RUNNING
                )
        self.logger.debug(
            f"finishing loop iteration sleeping {self.conf.runner_loop_sleep_time_sec=}"
        )
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation: "DistributedInvocation",
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the running process and registers it to wait for the results of specified invocations.
        :param running_invocation: The invocation that is waiting for results.
        :param result_invocations: A list of invocations whose results are being awaited.
        :param runner_args: Additional arguments required for the ProcessRunner.
        """
        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.PAUSED
        )
        if not result_invocations:
            return
        if not runner_args:
            raise RunnerError("runner_args should be defined for ProcessRunner")
        self.parse_args(runner_args)
        for result_invocation in result_invocations:
            if result_invocation not in self.wait_invocation:
                self.wait_invocation[result_invocation] = set()
            self.logger.debug(
                f"Invocation {running_invocation.invocation_id} is waiting for invocation {result_invocation.invocation_id} to finish"
            )
            self.wait_invocation[result_invocation].add(running_invocation)
