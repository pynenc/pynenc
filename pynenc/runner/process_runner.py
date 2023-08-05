from multiprocessing import Process, cpu_count, Condition, Manager, managers
import time
from typing import TYPE_CHECKING, Optional, NamedTuple

from .base_runner import BaseRunner
from pynenc.invocation import DistributedInvocation, InvocationStatus

from .base_runner import BaseRunner


class ProcessInfo(NamedTuple):
    process: Process
    invocation: DistributedInvocation


if TYPE_CHECKING:
    from multiprocessing.synchronize import Condition as ConditionBase
    from ..invocation import DistributedInvocation


class ProcessRunner(BaseRunner):
    wait_conditions: dict["DistributedInvocation", "ConditionBase"]
    wait_invocation: dict["DistributedInvocation", set["DistributedInvocation"]]
    processes: dict[int, ProcessInfo]

    max_processes: int
    waiting_processes: int

    def _on_start(self) -> None:
        self.wait_conditions = Manager().dict()  # type: ignore
        self.wait_invocation = Manager().dict()  # type: ignore
        self.processes = {}
        self.max_processes = cpu_count()
        self.waiting_processes = 0

    def _on_stop(self) -> None:
        """kill all the running processes and change invocation status to retry"""
        for process_info in self.processes.values():
            process_info.process.kill()
            self.app.orchestrator.set_invocation_status(
                process_info.invocation, InvocationStatus.RETRY
            )

    @property
    def available_processes(self) -> int:
        for pid in list(self.processes.keys()):
            if not self.processes[pid].process.is_alive():
                del self.processes[pid]
        return self.max_processes - len(self.processes) - self.waiting_processes

    def runner_loop_iteration(self) -> None:
        for invocation in self.app.orchestrator.get_invocations_to_run(
            max_num_invocations=self.available_processes
        ):
            process = Process(target=invocation.run, daemon=True)
            process.start()
            if process.pid:
                self.processes[process.pid] = ProcessInfo(process, invocation)
            else:
                ...
                # TODO if for mypy, the process should have a pid after start, otherwise it should raise an exception

        for invocation in list(self.wait_conditions.keys()):
            if invocation.status.is_final():
                with self.wait_conditions[invocation]:
                    self.wait_conditions[invocation].notify_all()
                self.wait_conditions.pop(invocation, None)

                waiting_invocations = self.wait_invocation.pop(invocation)
                self.app.orchestrator.set_invocations_status(
                    list(waiting_invocations), InvocationStatus.RUNNING
                )
                self.waiting_processes -= len(waiting_invocations)
        time.sleep(1)

    def waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocations: list["DistributedInvocation"],
    ) -> None:
        if not running_invocation:
            time.sleep(1)
            return

        self.app.orchestrator.set_invocation_status(
            running_invocation, InvocationStatus.PAUSED
        )

        for result_invocation in result_invocations:
            if result_invocation not in self.wait_invocation:
                self.wait_invocation[result_invocation] = set()
            self.wait_invocation[result_invocation].add(running_invocation)

            if result_invocation not in self.wait_conditions:
                self.wait_conditions[result_invocation] = Condition()

            self.waiting_processes += 1
            with self.wait_conditions[result_invocation]:
                self.wait_conditions[result_invocation].wait()
