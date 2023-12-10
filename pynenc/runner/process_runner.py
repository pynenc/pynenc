import os
import signal
import time
from multiprocessing import Manager, Process, cpu_count
from typing import TYPE_CHECKING, Any, Optional

from pynenc.invocation import InvocationStatus

from ..exceptions import RunnerError
from .base_runner import BaseRunner

if TYPE_CHECKING:
    from ..invocation import DistributedInvocation


class ProcessRunner(BaseRunner):
    wait_invocation: dict["DistributedInvocation", set["DistributedInvocation"]]
    processes: dict["DistributedInvocation", Process]

    max_processes: int

    @property
    def runner_args(self) -> dict[str, Any]:
        return {"wait_invocation": self.wait_invocation}

    @property
    def waiting_processes(self) -> int:
        if not self.wait_invocation:
            return 0
        return len(set.union(*self.wait_invocation.values()))

    def parse_args(self, args: dict[str, Any]) -> None:
        self.wait_invocation = args["wait_invocation"]

    def _on_start(self) -> None:
        self.wait_invocation = Manager().dict()  # type: ignore
        self.processes = {}
        self.max_processes = cpu_count()

    def _on_stop(self) -> None:
        """kill all the running processes and change invocation status to retry"""
        for invocation, process in self.processes.items():
            process.kill()
            self.app.orchestrator.set_invocation_status(
                invocation, InvocationStatus.RETRY
            )

    @property
    def available_processes(self) -> int:
        for invocation in list(self.processes.keys()):
            if not self.processes[invocation].is_alive():
                del self.processes[invocation]
        # discount waiting processes, they should do nothing
        # until the blocking invocation is finished
        # otherwise, running one worker with one process
        # will be lock indefintely until the blocking invocation runs
        return self.max_processes - len(self.processes)  # - self.waiting_processes

    def runner_loop_iteration(self) -> None:
        # called from parent process memory space
        for invocation in self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_processes
        ):
            process = Process(
                target=invocation.run,
                kwargs={"runner_args": self.runner_args},
                daemon=True,
            )
            process.start()
            self.logger.debug(
                f"Running invocation {invocation.invocation_id} on {process.pid=}"
            )
            if process.pid:
                self.processes[invocation] = process
            else:
                ...
                # TODO if for mypy, the process should have a pid after start, otherwise it should raise an exception

        for invocation in list(self.wait_invocation.keys()):
            is_final = invocation.status.is_final()
            for waiting_invocation in self.wait_invocation[invocation]:
                if pid := self.processes[waiting_invocation].pid:
                    if is_final:
                        os.kill(pid, signal.SIGCONT)
                    else:
                        os.kill(pid, signal.SIGSTOP)
            if is_final:
                waiting_invocations = self.wait_invocation.pop(invocation)
                self.logger.debug(
                    f"{invocation=} on final {invocation.status=}, resuming {waiting_invocations=}"
                )
                self.app.orchestrator.set_invocations_status(
                    list(waiting_invocations), InvocationStatus.RUNNING
                )
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        # called from subprocess memory space
        if not running_invocation:
            time.sleep(self.conf.invocation_wait_results_sleep_time_sec)
            return

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
