import time
from enum import Enum
from functools import cached_property
from multiprocessing import Manager, Process, cpu_count
from typing import TYPE_CHECKING, Any, NamedTuple

from pynenc import context
from pynenc.conf.config_runner import ConfigMultiThreadRunner
from pynenc.runner.base_runner import BaseRunner
from pynenc.runner.runner_context import RunnerContext
from pynenc.runner.thread_runner import ThreadRunner
from pynenc.util.multiprocessing_utils import warn_missing_main_guard


if TYPE_CHECKING:
    from pynenc.app import Pynenc


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
    parent_ctx_json: str,
    child_runner_id: str,
    runner_cache: dict,
    shared_status: dict[str, ProcessStatus],
) -> None:
    """
    Entry point for ThreadRunner worker processes spawned by MultiThreadRunner.

    The parent pre-generates the child_runner_id before spawning, enabling parent-based
    health reporting. The parent reports heartbeats for alive children via its main loop.
    """
    parent_ctx = RunnerContext.from_json(parent_ctx_json)
    runner_ctx = parent_ctx.new_child_context(
        ThreadRunner.__name__, runner_id=child_runner_id
    )
    app.runner._register_new_child_runner_context(runner_ctx)
    runner = ThreadRunner(app, runner_cache, runner_context=runner_ctx)
    # Replace the MultiThreadRunner with ThreadRunner in this process
    context.set_runner_context(app.app_id, runner_ctx)
    runner._on_start()
    app.logger.info(f"ThreadRunner process {child_runner_id} started")
    try:
        while True:
            # Clean up finished threads.
            runner.threads = {
                k: v for k, v in runner.threads.items() if v.thread.is_alive()
            }
            active_count = len(runner.threads)
            state = ProcessState.ACTIVE if active_count > 0 else ProcessState.IDLE
            shared_status[child_runner_id] = ProcessStatus(
                time.time(), active_count, state
            )
            app.logger.debug(
                f"Process {child_runner_id}: {active_count} active threads, state={state}"
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

    child_runner_ids: dict[str, Process]  # Maps child runner_id to Process
    manager: Manager  # type: ignore
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

        :return: False, as each thread runs in a separate process with independent memory
        """
        return False

    @property
    def max_parallel_slots(self) -> int:
        """
        The maximum number of parallel tasks that the runner can handle.

        :return: int representing the maximum number of parallel tasks
        """
        return max(self.conf.min_processes, self.max_processes)

    def get_active_child_runner_ids(self) -> list[str]:
        """Return runner_ids of child processes that are still alive."""
        return [
            runner_id
            for runner_id, proc in self.child_runner_ids.items()
            if proc.is_alive()
        ]

    def _on_start(self) -> None:
        """
        Initialize multiprocessing infrastructure for spawning worker processes.

        Validates that multiprocessing is being used safely before creating
        the Manager and spawning initial processes.
        """
        self.logger.info("Starting MultiThreadRunner")
        warn_missing_main_guard()
        self.manager = Manager()
        self.shared_status = self.manager.dict()  # type: ignore
        self.runner_cache = self._runner_cache or self.manager.dict()  # type: ignore
        self.child_runner_ids = {}
        self.max_processes = self.conf.max_processes or cpu_count()
        for _ in range(self.conf.min_processes):
            self._spawn_thread_runner_process()

    def _spawn_thread_runner_process(self) -> None:
        """Spawn a new ThreadRunner worker process with pre-generated runner_id."""
        import uuid

        child_runner_id = str(uuid.uuid4())
        args = {
            "app": self.app,
            "parent_ctx_json": self.runner_context.to_json(),
            "child_runner_id": child_runner_id,
            "runner_cache": self.runner_cache,
            "shared_status": self.shared_status,
        }
        p = Process(target=thread_runner_process_main, kwargs=args, daemon=True)
        p.start()
        self.child_runner_ids[child_runner_id] = p
        # Initialize shared_status with current time, 0 active threads, state IDLE.
        self.shared_status[child_runner_id] = ProcessStatus(
            time.time(), 0, ProcessState.IDLE
        )
        self.logger.info(
            f"Spawned ThreadRunner process {child_runner_id} with pid {p.pid}"
        )

    def _on_stop(self) -> None:
        """Stop all worker processes and shutdown the manager."""
        self.logger.info("Stopping MultiThreadRunner")
        for runner_id, proc in self.child_runner_ids.items():
            if proc.is_alive():
                proc.terminate()
                proc.join()
                self.logger.info(f"Terminated process {runner_id}")
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
        """Internal method called after receiving a signal to stop the runner loop."""
        self.logger.info("Stopping MultiThreadRunner loop")
        if hasattr(self, "child_runner_ids") and self.child_runner_ids is not None:
            for runner_id, proc in list(self.child_runner_ids.items()):
                try:
                    if proc.is_alive():
                        proc.terminate()
                        proc.join()
                        self.child_runner_ids.pop(runner_id, None)
                        self._safe_remove_shared_state(runner_id)
                        self.logger.info(
                            f"Terminated process {runner_id} during loop stop"
                        )
                except AssertionError:
                    self.logger.info(
                        f"Skipping process {runner_id} termination - not a child process"
                    )
        self.logger.info("MultiThreadRunner loop stopped")

    def _cleanup_dead_processes(self) -> None:
        """Remove processes that are no longer alive from tracking dictionaries."""
        dead_ids = [
            rid for rid, proc in self.child_runner_ids.items() if not proc.is_alive()
        ]
        if dead_ids:
            self.logger.warning(f"Found {len(dead_ids)} dead processes to clean up")
            for runner_id in dead_ids:
                self.child_runner_ids.pop(runner_id, None)
                self._safe_remove_shared_state(runner_id)
                self.logger.info(f"Cleaned up dead process {runner_id}")

    def _scale_up_processes(self) -> None:
        """Spawn new processes based on enforce_max_processes setting and pending tasks."""
        current_processes = len(self.child_runner_ids)
        if self.conf.enforce_max_processes:
            while current_processes < self.max_processes:
                self._spawn_thread_runner_process()
                current_processes = len(self.child_runner_ids)
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
        """Terminate processes that are idle longer than the configured timeout."""
        if (
            self.conf.enforce_max_processes
            and len(self.child_runner_ids) > self.max_processes
        ):
            return
        now = time.time()
        ids_to_remove: list[str] = []
        for runner_id, proc in self.child_runner_ids.items():
            if (
                len(self.child_runner_ids) - len(ids_to_remove)
                <= self.conf.min_processes
            ):
                break
            if (status := self.shared_status.get(runner_id)) is None:
                continue
            if status.is_idle(now, self.conf.idle_timeout_process_sec):
                idle_time = now - status.last_update
                self.logger.info(f"Process {runner_id} {idle_time=} sec, terminating.")
                proc.terminate()
                proc.join()
                ids_to_remove.append(runner_id)
        for runner_id in ids_to_remove:
            self.child_runner_ids.pop(runner_id, None)
            self._safe_remove_shared_state(runner_id)

    def runner_loop_iteration(self) -> None:
        """Execute one iteration of the runner loop."""
        self._scale_up_processes()
        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation_id: str,
        result_invocation_ids: list[str],
        runner_args: dict[str, Any] | None = None,
    ) -> None:
        """
        Handle waiting for results when called outside a process context.

        This method warns if called directly on MultiThreadRunner, as result waiting
        should occur within a ThreadRunner process using the context-set runner.

        :param str running_invocation_id: ID of the invocation waiting for results
        :param list[str] result_invocation_ids: IDs of invocations being awaited
        :param dict[str, Any] | None runner_args: Additional runner-specific arguments
        """
        del running_invocation_id, result_invocation_ids, runner_args
        self.logger.warning(self.WAITING_FOR_RESULTS_WARNING)
