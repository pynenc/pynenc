import os
from functools import cached_property


class Config:
    def __init__(self) -> None:
        # self._dev_mode_force_sync_tasks = False
        pass

    @cached_property
    def dev_mode_force_sync_tasks(self) -> bool:
        return bool(os.environ.get("DEV_MODE_FORCE_SYNC_TASK", False))
