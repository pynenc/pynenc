import time
from typing import NamedTuple

from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType

app = Pynenc()
app.conf.runner_cls = "ThreadRunner"


class SleepResult(NamedTuple):
    start: float
    end: float


@app.task(running_concurrency=ConcurrencyControlType.DISABLED)
def sleep_without_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())


@app.task(running_concurrency=ConcurrencyControlType.TASK)
def sleep_with_running_concurrency(seconds: float) -> SleepResult:
    start = time.time()
    time.sleep(seconds)
    return SleepResult(start=start, end=time.time())
