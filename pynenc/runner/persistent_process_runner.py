import multiprocessing
import os
import signal
import time
from functools import cached_property
from multiprocessing import Manager, Process
from typing import TYPE_CHECKING, Any, Optional

# Use 'spawn' method on macOS to avoid connection issues
if (
    hasattr(multiprocessing, "get_start_method")
    and multiprocessing.get_start_method(allow_none=True) != "spawn"
):
    try:
        multiprocessing.set_start_method("spawn", force=True)
    except RuntimeError:
        # Method already set and we're not in main process
        pass

from pynenc import context
from pynenc.conf.config_runner import ConfigPersistentProcessRunner
from pynenc.runner.base_runner import BaseRunner

if TYPE_CHECKING:
    from multiprocessing.synchronize import Event

    from pynenc.app import Pynenc
    from pynenc.invocation.dist_invocation import DistributedInvocation


def persistent_process_main(
    app: "Pynenc", *, process_key: str, runner_cache: dict, stop_event: "Event"
) -> None:
    """Main function for persistent process that executes invocations sequentially."""
    app.logger.info(f"Persistent process {process_key} started with PID {os.getpid()}")
    app.runner._runner_cache = runner_cache
    app.runner.set_extra_id(process_key)
    context.set_current_runner(app.app_id, app.runner)

    def handle_terminate(signum: int, frame: Any) -> None:
        app.logger.info(f"Process {process_key} received SIGTERM, setting stop event")
        stop_event.set()

    signal.signal(signal.SIGTERM, handle_terminate)  # Handle SIGTERM gracefully
    try:
        while not stop_event.is_set():
            invocations = list(
                app.orchestrator.get_invocations_to_run(max_num_invocations=1)
            )
            if not invocations:
                continue
            invocation = invocations[0]
            invocation_id = invocation.invocation_id
            app.logger.info(
                f"{process_key} starting invocation:{invocation.invocation_id}"
            )

            try:
                invocation.run()
            except Exception:
                app.logger.exception(f"Error executing invocation {invocation_id}")
    except KeyboardInterrupt:
        app.logger.info(f"Process {process_key} received KeyboardInterrupt, exiting")
    except Exception as e:
        app.logger.exception(f"Process {process_key} error: {e}")
    finally:
        app.logger.info(f"Process {process_key} shutting down")


class PersistentProcessRunner(BaseRunner):
    """
    PersistentProcessRunner maintains a fixed number of processes that continuously run tasks.
    """

    processes: dict[str, Process]
    manager: Manager  # type: ignore
    runner_cache: dict
    num_processes: int
    stop_event: "Event"

    @cached_property
    def conf(self) -> ConfigPersistentProcessRunner:
        return ConfigPersistentProcessRunner(
            config_values=self.app.config_values,
            config_filepath=self.app.config_filepath,
        )

    @property
    def cache(self) -> dict:
        """Returns the shared cache for all processes."""
        return self.runner_cache

    @staticmethod
    def mem_compatible() -> bool:
        """Indicates if the runner supports in-memory components."""
        return False

    @property
    def max_parallel_slots(self) -> int:
        """Returns the maximum number of concurrent processes."""
        return self.num_processes

    def _generate_process_key(self) -> str:
        """Generates a unique process key using runner_id and an incrementing counter."""
        self._process_id_counter += 1
        return f"{self.runner_id}-worker-{self._process_id_counter}"

    def _on_start(self) -> None:
        """Initializes the runner and spawns initial processes."""
        self.num_processes = max(
            self.conf.min_parallel_slots, self.conf.num_processes or os.cpu_count() or 1
        )
        self.logger.info(
            f"Starting PersistentProcessRunner with {self.num_processes} processes"
        )
        self.processes = {}
        self._process_id_counter: int = 0
        self.manager = Manager()
        self.runner_cache = self._runner_cache or self.manager.dict()  # type: ignore
        self.stop_event = self.manager.Event()  # type: ignore
        for _ in range(self.num_processes):
            self._spawn_persistent_process()

    def _spawn_persistent_process(self) -> str:
        """Spawns a new persistent process and returns its key."""
        if not hasattr(self, "running") or not self.running:
            raise RuntimeError("Trying to spawn new process after stopping loop")
        process_key = self._generate_process_key()
        args = {
            "app": self.app,
            "process_key": process_key,
            "runner_cache": self.runner_cache,
            "stop_event": self.stop_event,
        }
        p = Process(target=persistent_process_main, kwargs=args, daemon=True)
        try:
            p.start()
            self.processes[process_key] = p
            self.logger.info(
                f"Spawned persistent process {process_key} with pid {p.pid}"
            )
        except Exception as e:
            self.logger.error(f"Failed to spawn process {process_key}: {e}")
            raise
        return process_key

    def _terminate_all_processes(self) -> None:
        """Terminates all running processes with graceful shutdown attempt."""
        # Check if stop_event is initialized first
        if hasattr(self, "stop_event"):
            try:
                self.stop_event.set()  # Signal all processes to stop
            except Exception as e:
                self.logger.warning(f"Failed to set stop event: {e}")

        # Continue with process termination regardless
        for key, process in list(self.processes.items()):
            try:
                if process.is_alive():
                    process.terminate()
                    process.join(timeout=5)
                    if process.is_alive():
                        self.logger.warning(
                            f"Process {key} did not terminate, forcing kill"
                        )
                        process.kill()
                    self.processes.pop(key, None)
                    self.logger.debug(f"Terminated process {key}")
            except Exception as e:
                self.logger.warning(f"Error terminating process {key}: {e}")

    def _on_stop(self) -> None:
        """Cleans up all resources when runner stops."""
        try:
            self.logger.info("Stopping PersistentProcessRunner")
            self._terminate_all_processes()

            # Check if manager is initialized
            if hasattr(self, "manager"):
                try:
                    self.manager.shutdown()  # type: ignore
                except Exception as e:
                    self.logger.warning(f"Failed to shutdown manager: {e}")

            self.logger.info("PersistentProcessRunner stopped")
        except Exception as e:
            self.logger.error(f"Error during runner stop: {e}")

    def _on_stop_runner_loop(self) -> None:
        """Handles immediate stop from signal."""
        try:
            self.logger.info("Stopping PersistentProcessRunner loop due to signal")
            self._terminate_all_processes()
        except Exception as e:
            self.logger.error(f"Error during runner loop stop: {e}")

    def runner_loop_iteration(self) -> None:
        """Maintains the configured number of running processes."""
        dead_keys = [key for key, proc in self.processes.items() if not proc.is_alive()]
        for key in dead_keys:
            self.logger.warning(f"Detected dead process {key}, cleaning up")
            self.processes.pop(key, None)

        current_count = len(self.processes)
        if current_count < self.num_processes:
            processes_to_spawn = self.num_processes - current_count
            self.logger.info(f"Spawning {processes_to_spawn} new processes")
            for _ in range(processes_to_spawn):
                self._spawn_persistent_process()

        time.sleep(self.conf.runner_loop_sleep_time_sec)

    def _waiting_for_results(
        self,
        running_invocation: "DistributedInvocation",
        result_invocations: list["DistributedInvocation"],
        runner_args: Optional[dict[str, Any]] = None,
    ) -> None:
        """
        In this simplified version, we don't pause/resume processes.
        The invocation will just be marked as paused and the process will continue
        with other invocations.
        """
        del running_invocation, result_invocations, runner_args
        time.sleep(self.conf.invocation_wait_results_sleep_time_sec)
        # We cannot mark as PAUSED as the runner will not resume
