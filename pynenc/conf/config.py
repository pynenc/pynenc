import os
from functools import cached_property


class Config:
    def __init__(self) -> None:
        # self._dev_mode_force_sync_tasks = False
        pass

    @cached_property
    def dev_mode_force_sync_tasks(self) -> bool:
        return bool(os.environ.get("PYNENC_DEV_MODE_FORCE_SYNC_TASK", False))

    @cached_property
    def cycle_control(self) -> bool:
        return bool(os.environ.get("PYNENC_CYCLE_CONTROL", True))

    @cached_property
    def blocking_control(self) -> bool:
        return bool(os.environ.get("PYNENC_BLOCKING_CONTROL", True))

    @cached_property
    def max_pending_seconds(self) -> float:
        """max pending time that a task can be in pending state before reverting to registered state"""
        return float(os.environ.get("PYNENC_MAX_PENDING_SECONDS", 5.0))

    @cached_property
    def orchestrator_auto_final_invocation_purge_hours(self) -> float:
        """after how many hours should the orchestrator automatically purge invocations in final state"""
        return float(os.environ.get("PYNENC_ORC_AUTO_INV_FINAL_PURGE", 24.0))

    @cached_property
    def logging_level(self) -> str:
        return os.environ.get("PYNENC_LOGGING_LEVEL", "info")


# TODO
# - [global setting]store full invocation in router (or just id?)
#   (same for all components) for big tasks maybe better to store args only in state backend and call them each time
# - [task setting/global default] max concurrency for router
# - [component setting] redis connection
# - [globa setting] test_mode_disable_args_hash (for testing) if true show directly arg string to debug tests
