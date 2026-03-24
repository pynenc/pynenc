:::{image} \_static/logo.png
:alt: Pynenc
:align: center
:height: 90px
:class: hero-logo no-scaled-link
:::

# Pynenc

**Distributed task orchestration for Python.**

Pynenc manages task execution across distributed Python processes. Tasks are regular
Python functions — decorated, called like normal code, and backed by pluggable
infrastructure. The orchestrator handles dependency ordering, concurrency, lifecycle
tracking, and recovery automatically.

```bash
pip install pynenc
```

---

## A Taste of the API

Write tasks the way you write Python. Pynenc handles the rest:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y

# Call it — get an invocation back, block for the result
result = add(1, 2).result   # 3
```

Tasks can call other tasks. Dependencies are resolved automatically — no manual
wiring, no DAG configuration:

```python
@app.task
def fibonacci(n: int) -> int:
    if n <= 1:
        return n
    return fibonacci(n - 1).result + fibonacci(n - 2).result
```

When `fibonacci(10)` runs, the orchestrator fans out sub-tasks, pauses them while
they wait for each other, and resumes them without blocking any runner thread.
See {doc}`usage_guide/use_case_004_auto_orchestration`.

---

## Key Capabilities

:::::{grid} 1 2 2 2
:gutter: 3

::::{grid-item-card} Invocation Lifecycle
Every task call becomes a tracked **invocation** through a validated state
machine: `REGISTERED → PENDING → RUNNING → SUCCESS / FAILED`. Tasks that
block on a dependency transition through `PAUSED → RESUMED` without holding
a runner thread. Status, ownership, results, and exceptions are all
persisted — no silent drops, no double execution.

{doc}`usage_guide/invocation_status`
::::

::::{grid-item-card} Concurrency Control
Prevent duplicate work at two independent points: _registration_ (should
this call even be enqueued?) and _execution_ (should two instances run
concurrently?). Four modes — `DISABLED`, `TASK`, `ARGUMENTS`, `KEYS` —
match the argument granularity you need. Each decorator configures them
independently.

{doc}`usage_guide/use_case_003_concurrency_control`
::::

::::{grid-item-card} Trigger System
Schedule tasks declaratively: cron expressions, task status transitions,
result conditions, or custom events registered via decorator. Trigger state
is distributed — backends include `MemTrigger` for tests, `SqliteTrigger`
for single-host, and `RedisTrigger` / `MongoTrigger` for production.

{doc}`usage_guide/use_case_010_trigger_system`
::::

::::{grid-item-card} Runner Recovery & Core Services
Workers register a **heartbeat** on a configurable interval. If a runner
process dies, the orchestrator detects the missing heartbeat and
automatically re-queues its invocations — tasks stuck in `RUNNING` become
recoverable without operator intervention. An **atomic service** framework
ensures only one runner executes recovery and trigger evaluation at a time.

{doc}`reference/core_services`
::::

:::::

---

## Pluggable Backends

The core ships with in-memory and SQLite backends. Production backends are
separate installable packages — swap them without touching application code:

| Plugin       | Install                       | Provides                  |
| ------------ | ----------------------------- | ------------------------- |
| **Redis**    | `pip install pynenc-redis`    | All components on Redis   |
| **MongoDB**  | `pip install pynenc-mongodb`  | All components on MongoDB |
| **RabbitMQ** | `pip install pynenc-rabbitmq` | Broker on RabbitMQ        |

`PynencBuilder` makes mixing plugins explicit and readable:

```python
from pynenc import PynencBuilder

app = (
    PynencBuilder()
    .app_id("my_app")
    .redis(url="redis://localhost:6379")   # orchestrator + state + trigger
    .rabbitmq_broker(host="rabbitmq")      # swap broker to RabbitMQ
    .process_runner()
    .build()
)
```

---

## Testing Without Infrastructure

Pynenc has two testing modes — pick the one that fits the test:

**Sync shortcut** — set `PYNENC__DEV_MODE_FORCE_SYNC_TASKS=True` (or
`app.conf.dev_mode_force_sync_tasks = True` in `setUp`). The task
decorator becomes transparent: calling a task runs its function directly
and returns the result inline. Zero infrastructure, zero threads — but
also no concurrency control, no pause/resume, and no deadlock detection.
Good for unit-testing pure task logic.

```python
def setUp(self):
    tasks.app.conf.dev_mode_force_sync_tasks = True
```

**Full in-memory stack** — replace the app with a freshly built instance
backed by all `Mem*` components and a `ThreadRunner`. The full orchestration
loop runs in-process: auto-pause/resume, concurrency control, trigger
evaluation, and recovery all behave exactly as in production, with no
external services. Using `PynencBuilder` avoids stale cached components that
would remain if you patched `conf` attributes on an already-instantiated app.

```python
def setUp(self):
    tasks.app = PynencBuilder().memory().thread_runner().build()
    self.thread = threading.Thread(target=tasks.app.runner.run, daemon=True)
    self.thread.start()

def tearDown(self):
    tasks.app.runner.stop_runner_loop()
    self.thread.join()
```

![Running a local task in memory with ThreadRunner](https://github.com/user-attachments/assets/c4ae1c20-1c91-4848-a1bd-1445a4d40174)

{doc}`usage_guide/use_case_005_sync_unit_testing` · {doc}`usage_guide/use_case_006_mem_unit_testing`

---

## Built-in Monitoring

Pynmon ships with Pynenc and gives you deep, real-time visibility into your distributed execution — no external tooling required.

::::{grid} 1 2 2 2
:gutter: 3

:::{grid-item}

```{image} _static/pynmon_dashboard.png
:alt: Pynmon dashboard
:width: 100%
```

:::

:::{grid-item}

```{image} _static/pynmon_timeline.png
:alt: Pynmon execution timeline
:width: 100%
```

:::

::::

{doc}`monitoring/index`

---

## Where to Go Next

::::{grid} 1 2 2 3
:gutter: 3
:padding: 0
:class-container: sd-mt-1

:::{grid-item-card} 🚀 Getting Started
:link: getting_started/index
:link-type: doc
:shadow: sm
From zero to a working distributed task — step by step.
:::

:::{grid-item-card} 📖 Usage Guide
:link: usage_guide/index
:link-type: doc
:shadow: sm
Concurrency, orchestration, workflows, triggers, serializers, testing.
:::

:::{grid-item-card} ⚙️ Configuration
:link: configuration/index
:link-type: doc
:shadow: sm
Every setting for every component, with defaults and env-var names.
:::

:::{grid-item-card} 🖥️ Monitoring
:link: monitoring/index
:link-type: doc
:shadow: sm
Pynmon web UI — timeline, invocation explorer, runner health.
:::

:::{grid-item-card} 🔌 Plugin Reference
:link: reference/plugins
:link-type: doc
:shadow: sm
How to build your own backend plugin.
:::

:::{grid-item-card} 📚 API Reference
:link: apidocs/index
:link-type: doc
:shadow: sm
Auto-generated docs for all public classes and functions.
:::
::::

```{toctree}
:hidden:
:maxdepth: 2
:caption: Learn

getting_started/index
usage_guide/index
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Understand

overview
faq
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Reference

configuration/index
cli/index
monitoring/index
reference/builder
reference/runners
reference/core_services
reference/serializers
apidocs/index.rst
```

```{toctree}
:hidden:
:caption: Backend Plugins

Redis <https://pynenc-redis.readthedocs.io/>
MongoDB <https://pynenc-mongodb.readthedocs.io/>
RabbitMQ <https://pynenc-rabbitmq.readthedocs.io/>
reference/plugins
```

```{toctree}
:hidden:
:maxdepth: 2
:caption: Project

contributing/index
changelog
license
```
