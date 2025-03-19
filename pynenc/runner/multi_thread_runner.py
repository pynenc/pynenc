import time
from enum import Enum
from functools import cached_property
from multiprocessing import Manager, Process, cpu_count
from typing import TYPE_CHECKING, Any, NamedTuple, Optional

from pynenc import context
from pynenc.conf.config_runner import ConfigMultiThreadRunner
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.thread_runner import ThreadRunner

if TYPE_CHECKING:
    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation


class ProcessState(Enum):
    ACTIVE = "active"
    IDLE = "idle"


class ProcessStatus(NamedTuple):
    last_update: float
    active_count: int
    state: ProcessState

    def is_idle(self, now: float, idle_timeout: float) -> bool:
        """Return True if the process is idle and has been idle longer than idle_timeout."""
        return (
            now - self.last_update
        ) > idle_timeout and self.state == ProcessState.IDLE


def thread_runner_process_main(
    app: "Pynenc",
    *,
    runner_cache: dict,
    shared_status: dict[str, ProcessStatus],
    process_key: str,
) -> None:
    """
    MultiThreadRunner manages multiple processes, each running a ThreadRunner.

    Unlike ThreadRunner, which operates within a single process, MultiThreadRunner
    spawns separate processes to distribute workload. The global context dictionary
    (via context.py) is critical here because each process must maintain its own
    ThreadRunner instance in its thread-local storage. This prevents conflicts
    between processes and ensures that task executions within a process use the
    correct runner. The context check is not typically needed in ThreadRunner alone,
    as it operates in a single-threaded or single-process environment where the
    instance-level runner suffices.
    """
    runner = ThreadRunner(app, runner_cache, process_key)
    # Replace the MultiThreadRunner with ThreadRunner in this process
    context.set_current_runner(app.app_id, runner)
    runner._on_start()
    app.logger.info(f"ThreadRunner process {process_key} started")
    try:
        while True:
            # Clean up finished threads.
            runner.threads = {
                k: v for k, v in runner.threads.items() if v.thread.is_alive()
            }
            active_count = len(runner.threads)
            state = ProcessState.ACTIVE if active_count > 0 else ProcessState.IDLE
            shared_status[process_key] = ProcessStatus(time.time(), active_count, state)
            app.logger.debug(
                f"Process {process_key}: {active_count} active threads, state={state}"
            )
            runner.runner_loop_iteration()
    except KeyboardInterrupt:
        pass
    finally:
        runner._on_stop()


class MultiThreadRunner(BaseRunner):
    """
    MultiThreadRunner spawns separate processes, each running a ThreadRunner.
    It scales processes based on pending invocations and terminates those that remain idle.
    """

    WAITING_FOR_RESULTS_WARNING = (
        "waiting_for_results called on MultiThreadRunner from within a task. "
        "This should be handled by the ThreadRunner instance in the process."
    )

    processes: dict[str, Process]
    manager: Manager  # type: ignore
    # shared_status: Maps process key to ProcessStatus
    shared_status: dict[str, ProcessStatus]
    max_processes: int
    runner_cache: dict

    @cached_property
    def conf(self) -> ConfigMultiThreadRunner:
        return ConfigMultiThreadRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def cache(self) -> dict:
        """Returns the shared cache for all processes."""
        return self.runner_cache

    @staticmethod
    def mem_compatible() -> bool:
        """
        Indicates if the runner is compatible with in-memory components.
        :return: False, as each Thread runs in a separate process with independent memory.
        """
        return False

    @property
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.
        :return: An integer representing the maximum number of parallel tasks, based on config or CPU count.
        """
        return max(self.conf.min_processes, self.max_processes)

    def _on_start(self) -> None:
        self.logger.info("Starting MultiThreadRunner")
        self.manager = Manager()
        self.shared_status = self.manager.dict()  # type: ignore
        self.runner_cache = self._runner_cache or self.manager.dict()  # type: ignore
        self.processes = {}
        self.max_processes = self.conf.max_processes or cpu_count()
        for _ in range(self.conf.min_processes):
            self._spawn_thread_runner_process()

    def _spawn_thread_runner_process(self) -> None:
        process_key = f"trp-{time.time()}-{len(self.processes)}"
        args = {
            "app": self.app,
            "runner_cache": self.runner_cache,
            "shared_status": self.shared_status,
            "process_key": process_key,
        }
        p = Process(target=thread_runner_process_main, kwargs=args, daemon=True)
        p.start()
        self.processes[process_key] = p
        # Initialize shared_status with current time, 0 active threads, state IDLE.
        self.shared_status[process_key] = ProcessStatus(
            time.time(), 0, ProcessState.IDLE
        )
        self.logger.info(f"Spawned ThreadRunner process {process_key} with pid {p.pid}")

    def _on_stop(self) -> None:
        self.logger.info("Stopping MultiThreadRunner")
        for key, proc in self.processes.items():
            if proc.is_alive():
                proc.terminate()
                proc.join()
                self.logger.info(f"Terminated process {key}")
        self.manager.shutdown()  # type: ignore
        self.logger.info("MultiThreadRunner stopped")

    def _safe_remove_shared_state(self, key: str) -> None:
        """
        Safely remove a process's shared state, handling manager shutdown cases.

        :param str key: The process key to remove from shared state
        """
        try:
            self.shared_status.pop(key, None)
        except (EOFError, BrokenPipeError):
            self.logger.debug(f"Manager already stopped while removing state for {key}")

    def _on_stop_runner_loop(self) -> None:
        """
        Internal method called after receiving a signal to stop the runner loop.
        """
        self.logger.info("Stopping MultiThreadRunner loop")
        # Terminate all processes immediately
        for key, proc in list(self.processes.items()):
            try:
                if proc.is_alive():
                    proc.terminate()
                    proc.join()
                    self.processes.pop(key, None)
                    self._safe_remove_shared_state(key)
                    self.logger.info(f"Terminated process {key} during loop stop")
            except AssertionError:
                # We're trying to check a non-child process (likely ourselves)
                self.logger.info(
                    f"Skipping process {key} termination - not a child process"
                )
        self.logger.info("MultiThreadRunner loop stopped")

    def _cleanup_dead_processes(self) -> None:
        """Remove processes that are no longer alive from our tracking dictionaries."""
        dead_keys = [key for key, proc in self.processes.items() if not proc.is_alive()]
        if dead_keys:
            self.logger.warning(f"Found {len(dead_keys)} dead processes to clean up")
            for key in dead_keys:
                self.processes.pop(key, None)
                self._safe_remove_shared_state(key)
                self.logger.info(f"Cleaned up dead process {key}")

    def _scale_up_processes(self) -> None:
        """Spawns new processes based on enforce_max_processes setting and pending tasks."""
        current_processes = len(self.processes)
        if self.conf.enforce_max_processes:
            # Always maintain exactly max_processes processes.
            while current_processes < self.max_processes:
                self._spawn_thread_runner_process()
                current_processes = len(self.processes)
        else:
            queued_invocations = self.app.broker.count_invocations()
            if (
                queued_invocations > current_processes
                and current_processes < self.max_processes
            ):
                to_spawn = min(
                    queued_invocations - current_processes,
                    self.max_processes - current_processes,
                )
                for _ in range(to_spawn):
                    self._spawn_thread_runner_process()

    def _terminate_idle_processes(self) -> None:
        """
        Terminates processes that are idle longer than the configured timeout.
        A process is considered idle if its shared status indicates IDLE and
        the time since its last update exceeds idle_timeout_process_sec.
        Respects enforce_max_processes if enabled.
        """
        # Skip termination if enforce_max_processes is enabled and we're at max
        if self.conf.enforce_max_processes and len(self.processes) > self.max_processes:
            return
        now = time.time()
        keys_to_remove: list[str] = []
        for key, proc in self.processes.items():
            if len(self.processes) - len(keys_to_remove) <= self.conf.min_processes:
                break
            if (status := self.shared_status.get(key)) is None:
                continue
            if status.is_idle(now, self.conf.idle_timeout_process_sec):
                idle_time = now - status.last_update
                self.logger.info(f"Process {key} {idle_time=} sec, terminating.")
                proc.terminate()
                proc.join()
                keys_to_remove.append(key)
        for key in keys_to_remove:
            self.processes.pop(key, None)
            self._safe_remove_shared_state(key)

    def runner_loop_iteration(self) -> None:
        # self._cleanup_dead_processes()
        self._scale_up_processes()
        # self._terminate_idle_processes()
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation: Optional["DistributedInvocation"],
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        Handle waiting for results when called outside a process context.

        This method warns if called directly on MultiThreadRunner, as result waiting
        should occur within a ThreadRunner process, which uses the context-set runner.
        The global context ensures each process handles its own results correctly.
        """
        del running_invocation, result_invocations, runner_args
        self.logger.warning(self.WAITING_FOR_RESULTS_WARNING)
