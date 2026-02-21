import os
import signal
from multiprocessing import Manager, Process, cpu_count
from typing import Any, NamedTuple, TYPE_CHECKING

from pynenc.exceptions import InvocationStatusError, RunnerError
from pynenc.invocation import InvocationStatus
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.shutdown_diagnostics import log_runner_shutdown
from pynenc.util.multiprocessing_utils import warn_missing_main_guard

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation import DistributedInvocation
    from pynenc.identifiers.invocation_id import InvocationId
    from pynenc.runner.runner_context import RunnerContext


def run_invocation(
    app: "Pynenc",
    invocation: "DistributedInvocation",
    runner_ctx: "RunnerContext",
    runner_args: dict,
) -> None:
    """Run invocation in a separate process (must be top-level for multiprocessing)."""
    invocation.run(runner_ctx, runner_args=runner_args)


class ClassifiedInvocations(NamedTuple):
    """
    Categorized invocation IDs based on their status.

    :param final: List of invocation IDs that have reached their final status
    :param non_final: List of invocation IDs that are still in progress
    """

    final: list["InvocationId"]
    non_final: list["InvocationId"]


class ChildProcessInfo(NamedTuple):
    """
    Information about a child worker process.

    :param process: The Process object
    :param invocation_id: The invocation_id being executed by this process
    """

    process: Process
    invocation_id: "InvocationId"


class ProcessRunner(BaseRunner):
    """
    ProcessRunner is a concrete implementation of BaseRunner that executes tasks in separate processes.

    It manages task invocations, handling their execution, monitoring, and lifecycle within individual processes.
    This runner is suitable for CPU-bound tasks and scenarios where task isolation is essential.
    """

    wait_invocation: dict[
        "InvocationId", set["InvocationId"]
    ]  # Maps invocation_id to set of waiting invocation_ids
    child_runner_ids: dict[str, ChildProcessInfo]  # Maps runner_id to process info
    inv_id_to_runner_id: dict["InvocationId", str]  # Maps invocation_id to runner_id
    manager: Manager  # type: ignore

    max_processes: int

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

    def get_active_child_runner_ids(self) -> list[str]:
        """Return runner_ids of child processes that are still alive."""
        return [
            runner_id
            for runner_id, info in self.child_runner_ids.items()
            if info.process.is_alive()
        ]

    def _log_shutdown(self, signum: int | None) -> None:
        log_runner_shutdown(
            self.app.logger,
            self.__class__.__name__,
            self.runner_id,
            signum,
            processes={
                rid: (info.process, str(info.invocation_id))
                for rid, info in self.child_runner_ids.items()
            },
            waiting_inv_ids=[str(k) for k in (self.wait_invocation or {})],
        )

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
        warn_missing_main_guard()
        self.manager = Manager()
        self.wait_invocation = self.manager.dict()  # type: ignore
        self.runner_cache = self._runner_cache or self.manager.dict()  # type: ignore
        self.child_runner_ids = {}
        self.inv_id_to_runner_id = {}
        self.max_processes = cpu_count()

    def _on_stop(self) -> None:
        """
        Internal method called when the ProcessRunner stops.

        Kills each alive child with SIGKILL, waits for it to die, then calls
        _kill_and_reroute. This ordering avoids a race where the task finishes
        after we set KILLED but before the process actually dies: by joining
        first we know the process is gone, and _kill_and_reroute silently skips
        invocations that already reached a final status.
        """
        self.logger.info("Stopping ProcessRunner")
        for runner_id, info in self.child_runner_ids.items():
            if info.process.is_alive():
                self.logger.warning(
                    f"Killing runner {runner_id} (pid={info.process.pid}) "
                    f"with invocation {info.invocation_id}"
                )
                info.process.kill()
                info.process.join()
            # Reconstruct the child's RunnerContext so the ownership check passes:
            # the invocation was set to RUNNING under the child's runner_id, so
            # only a context carrying that same runner_id can transition it further.
            child_ctx = self.runner_context.new_child_context(
                "ProcessRunnerWorker", runner_id=runner_id
            )
            self._kill_and_reroute(info.invocation_id, runner_ctx=child_ctx)
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
        for runner_id in list(self.child_runner_ids):
            info = self.child_runner_ids[runner_id]
            if not info.process.is_alive():
                info.process.join()
                del self.child_runner_ids[runner_id]
                self.inv_id_to_runner_id.pop(info.invocation_id, None)
        # discount waiting processes, they should do nothing
        # until the blocking invocation is finished
        # otherwise, running one worker with one process
        # will be lock indefintely until the blocking invocation runs
        return self.max_parallel_slots - len(self.child_runner_ids)

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

    def _get_process_for_invocation(
        self, invocation_id: "InvocationId"
    ) -> Process | None:
        """Get the Process object for a given invocation_id."""
        if runner_id := self.inv_id_to_runner_id.get(invocation_id):
            if info := self.child_runner_ids.get(runner_id):
                return info.process
        return None

    def handle_waiting_invocations(self) -> None:
        """Handle the waiting invocations"""
        classified = self.clasify_waiting_invocations()
        # Pause processes waiting for non-final invocations
        for invocation_id in classified.non_final:
            for waiting_invocation_id in self.wait_invocation.get(invocation_id, []):
                if waiting_process := self._get_process_for_invocation(
                    waiting_invocation_id
                ):
                    if waiting_process.pid:
                        os.kill(waiting_process.pid, signal.SIGSTOP)
                        self.logger.info(
                            f"{waiting_invocation_id=} waiting for {invocation_id=}, pausing process {waiting_process.pid}"
                        )
        # Get the invocations that are waiting in finalized ones
        to_resume_invocation_ids: set[InvocationId] = set()
        for invocation_id in classified.final:
            if waiting_invocation_ids := self.wait_invocation.get(invocation_id, set()):
                to_resume_invocation_ids.update(waiting_invocation_ids)
                self.wait_invocation[invocation_id] = set()
                self.logger.info(
                    f"{invocation_id=} finalized, resuming waiting ones: {waiting_invocation_ids}"
                )
        # Resume the processes waiting for finalized invocations
        # and set their status to RESUMED
        if to_resume_invocation_ids:
            for waiting_invocation_id in to_resume_invocation_ids:
                # Find the process for this waiting invocation ID
                if waiting_process := self._get_process_for_invocation(
                    waiting_invocation_id
                ):
                    if waiting_process.pid:
                        os.kill(waiting_process.pid, signal.SIGCONT)
                        self.logger.info(
                            f"resuming process {waiting_process.pid} of {waiting_invocation_id=} "
                        )
                        try:
                            self.app.orchestrator.set_invocation_status(
                                waiting_invocation_id,
                                InvocationStatus.RESUMED,
                                self.runner_context,
                            )
                        except InvocationStatusError as ex:
                            self.logger.warning(
                                f"Could not set invocation {waiting_invocation_id} to RESUMED status: {ex}"
                            )

    def runner_loop_iteration(self) -> None:
        """
        Executes one iteration of the ProcessRunner loop.
        Handles the execution and monitoring of task invocations in separate processes.
        Each process gets a reserved runner context and only starts if an invocation is available.
        """
        self.logger.debug("starting runner loop iteration (dynamic process slots)")
        for _ in range(self.available_processes):
            # Reserve a unique runner context for this process
            reserved_ctx = self.runner_context.new_child_context("ProcessRunnerWorker")
            # Try to get an invocation for this reserved context
            invocations = list(
                self.app.orchestrator.get_invocations_to_run(1, reserved_ctx)
            )
            if not invocations:
                break
            invocation = invocations[0]
            self._register_new_child_runner_context(reserved_ctx)
            process = Process(
                target=run_invocation,
                args=(self.app, invocation, reserved_ctx, self.runner_args),
                daemon=True,
            )
            self.app.logger.info(
                f"{self.runner_id} starting invocation:{invocation.invocation_id} with reserved_ctx {reserved_ctx.runner_id}"
            )
            process.start()
            if process.pid:
                self.child_runner_ids[reserved_ctx.runner_id] = ChildProcessInfo(
                    process=process, invocation_id=invocation.invocation_id
                )
                self.inv_id_to_runner_id[invocation.invocation_id] = (
                    reserved_ctx.runner_id
                )
            else:
                # The recovery service should pick this up
                self.logger.error(
                    f"Failed to start process for invocation {invocation.invocation_id}"
                )
        self.handle_waiting_invocations()

    def _waiting_for_results(
        self,
        running_invocation_id: "InvocationId",
        result_invocation_ids: list["InvocationId"],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Handles invocations that are waiting for results from other invocations.
        Pauses the running process and registers it to wait for the results of specified invocations.

        :param InvocationId running_invocation_id: The ID of the invocation that is waiting for results.
        :param list[InvocationId] result_invocation_ids: A list of IDs of invocations whose results are being awaited.
        :param dict[str, Any] | None runner_args: Additional arguments required for the ProcessRunner.
        """
        if not result_invocation_ids:
            return
        if not runner_args:
            raise RunnerError("runner_args should be defined for ProcessRunner")
        try:
            self.parse_args(runner_args)
            self.app.orchestrator.set_invocation_status(
                running_invocation_id,
                InvocationStatus.PAUSED,
                runner_ctx=self.runner_context,
            )
            for result_inv_id in result_invocation_ids:
                current_waiters = set(self.wait_invocation.get(result_inv_id, set()))
                current_waiters.add(running_invocation_id)
                self.wait_invocation[result_inv_id] = current_waiters
                self.logger.debug(
                    f"Invocation {running_invocation_id} is waiting for invocation {result_inv_id} to finish"
                )
        except InvocationStatusError as ex:
            self.logger.warning(
                f"Not possible to change {running_invocation_id} status: {ex}"
            )
            # remove from any wait_invocation set
            for result_inv_id in result_invocation_ids:
                if running_invocation_id in self.wait_invocation.get(
                    result_inv_id, set()
                ):
                    self.wait_invocation[result_inv_id].remove(running_invocation_id)
            return
