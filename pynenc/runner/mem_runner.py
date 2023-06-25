from abc import ABC, abstractmethod
from typing import TYPE_CHECKING, Optional, Type

from .base_runner import BaseRunner

if TYPE_CHECKING:
    from ..app import Pynenc
    from ..invocation import DistributedInvocation
    from ..types import Params, Result

    from types import TracebackType


class MemRunner(BaseRunner):
    def on_start(self) -> None:
        pass

    def on_stop(self) -> None:
        pass

    def start_runner_loop(self) -> None:
        pass
