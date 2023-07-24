import os
from functools import cached_property


class Config:
    def __init__(self) -> None:
        # self._dev_mode_force_sync_tasks = False
        pass

    @cached_property
    def dev_mode_force_sync_tasks(self) -> bool:
        return bool(os.environ.get("DEV_MODE_FORCE_SYNC_TASK", False))


# TODO
# - [global setting]store full invocation in router (or just id?)
#   (same for all components) for big tasks maybe better to store args only in state backend and call them each time
# - [task setting/global default] max concurrency for router
# - [component setting] redis connection
