# Direct Task: Distribute Without Refactoring

## Overview

`@app.direct_task` is the migration-friendly counterpart of `@app.task`. It preserves
the function signature **and** the call site: the decorated function still returns its
value directly, exception handling works as before, and the caller waits for the result
the same way it would for a regular Python function. The function body runs on a
worker; everything else looks unchanged.

The full demo is in the [direct_task_demo](https://github.com/pynenc/samples/tree/main/direct_task_demo)
sample, which includes a `tasks_original.py` (plain Python) and a `tasks.py` (the same
code with decorators added) so the migration diff is explicit.

## When to Use Direct Task

| Decorator          | Returns                     | Caller blocks?               | Use when…                                              |
| ------------------ | --------------------------- | ---------------------------- | ------------------------------------------------------ |
| `@app.task`        | `Invocation`                | Only when reading `.result`  | Fire-and-forget, or fine-grained result handling       |
| `@app.direct_task` | The function's return value | Always, until result arrives | Distributing existing code without touching call sites |

## Basic Usage

The starting point is plain Python:

```python
import time
from hashlib import md5


def _build_report(period: str) -> dict:
    time.sleep(0.5)  # simulates DB queries + aggregation
    seed = int(md5(period.encode()).hexdigest()[:8], 16)
    revenue = 50_000 + (seed % 950_000)
    orders = 100 + (seed % 9_900)
    return {"period": period, "revenue": revenue, "orders": orders,
            "avg_order_value": round(revenue / orders, 2)}


def generate_report(period: str) -> dict:
    return _build_report(period)
```

Adding `@app.direct_task` is the only change required to make `generate_report`
distributable:

```python
from pynenc import Pynenc

app = Pynenc()


@app.direct_task
def generate_report(period: str) -> dict:
    return _build_report(period)
```

The caller is unchanged:

```python
report = generate_report("Q1-2025")
# {"period": "Q1-2025", "revenue": 477381, ...}
```

## Sync Mode for Local Development

Setting `PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True` (or `app.conf.dev_mode_force_sync_tasks = True`)
runs decorated functions inline in the calling thread. No runner, no broker, no
database writes. Behaviour is identical to the plain Python version, which is what
makes the env var the right tool for incremental migration: existing tests stay green
while decorators are added one by one.

```bash
PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True python my_script.py
```

Remove the variable in production to distribute work to runners automatically.

## Running with a Runner

For distributed execution, start a runner. The simplest pattern uses a background
thread:

```python
import threading
import tasks

runner_thread = threading.Thread(target=tasks.app.runner.run, daemon=True)
runner_thread.start()
try:
    report = tasks.generate_report("Q1-2025")
finally:
    tasks.app.runner.stop_runner_loop()
    runner_thread.join()
```

`stop_runner_loop()` must be called **before** `runner_thread.join()`, otherwise the
join will block indefinitely.

## Exception Propagation

Exceptions raised inside the task are re-raised at the call site, just like a regular
function:

```python
@app.direct_task
def fails() -> None:
    raise ValueError("intentional")

try:
    fails()
except ValueError as e:
    print(e)  # "intentional"
```

## Caller-Side Concurrency

`@app.direct_task` always blocks the caller, by design — that is what preserves the
calling contract of a regular Python function and keeps migration zero-cost. A plain
`for` loop is therefore sequential, exactly as before. For caller-side concurrency,
`ThreadPoolExecutor` is the standard Python pattern and composes naturally with
`direct_task`:

```python
from concurrent.futures import ThreadPoolExecutor

periods = ["Q1-2025", "Q2-2025", "Q3-2025", "Q4-2025"]
with ThreadPoolExecutor(max_workers=len(periods)) as pool:
    reports = list(pool.map(generate_report, periods))
```

Each thread blocks on its own call; the runner processes them in parallel.

## Single-Call Fan-Out: `parallel_func` + `aggregate_func`

When the parallelism belongs inside the function — the caller passes a list, expects a
list back, and should not have to manage a thread pool — declare it on the decorator.

The starting point is the batch version of the function, in plain Python:

```python
def generate_reports(periods: list[str]) -> list[dict]:
    return [_build_report(p) for p in periods]
```

Adding the decorator with `parallel_func` and `aggregate_func` distributes the work
across workers without changing the function body or the call site:

```python
def _per_period(args: dict) -> list[tuple[list[str]]]:
    """Split the caller's list of periods so each worker handles one."""
    return [([p],) for p in args["periods"]]


def _flatten(chunks: list[list[dict]]) -> list[dict]:
    return [report for chunk in chunks for report in chunk]


@app.direct_task(parallel_func=_per_period, aggregate_func=_flatten)
def generate_reports(periods: list[str]) -> list[dict]:
    return [_build_report(p) for p in periods]
```

The caller invokes it the same way as the plain Python version:

```python
reports = generate_reports(periods=["Q1-2025", "Q2-2025", "Q3-2025"])
# [{"period": "Q1-2025", ...}, {"period": "Q2-2025", ...}, {"period": "Q3-2025", ...}]
```

How the routing works:

1. `parallel_func` receives the caller's keyword arguments as a dict — here,
   `args = {"periods": [...]}`. It reads `args["periods"]` and yields one per-worker
   argument tuple per period.
2. Each worker runs the function body with its own slice of the input.
3. `aggregate_func` combines the per-worker return values into the final result.

`aggregate_func` is required when `parallel_func` is set; omitting it raises
`ValueError("Aggregation function required for parallel execution")`.

## Async Support

`@app.direct_task` works on `async def` functions. The wrapper returns an awaitable:

```python
@app.direct_task
async def async_generate_report(period: str) -> dict:
    return _build_report(period)

report = await async_generate_report("Q1-2025")
```

## Limitations

- **Always blocks the caller** — by design. For fire-and-forget semantics, use
  `@app.task` and call `.result` only when the value is needed.
- **No triggers** — `direct_task` does not accept the `triggers=` parameter.
- **Tasks must live in importable modules** — like all pynenc tasks, decorating a
  function inside a `__main__` module raises `RuntimeError`.

## Further Reading

- Sample: [direct_task_demo](https://github.com/pynenc/samples/tree/main/direct_task_demo) — `tasks_original.py` vs `tasks.py` and three runnable scripts
- Article: [Distribute your Python app without rewriting it](https://pynenc.org/2026/04/24/distribute-python-app-without-rewriting-it.html)
- Related: {doc}`./use_case_005_sync_unit_testing` for the dev-mode toggle in tests
