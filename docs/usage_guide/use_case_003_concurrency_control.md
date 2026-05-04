# Concurrency Control

## Overview

Pynenc lets you cap how many invocations of a task can be **registered** or
**running** at the same time, without an external lock service. The
orchestrator already tracks every invocation it accepts; checking whether a
new one would conflict with an existing one is the same kind of lookup.

This guide covers:

- The four concurrency _scopes_ (`DISABLED`, `TASK`, `ARGUMENTS`, `KEYS`).
- The two _flags_ that apply them (`registration_concurrency` at enqueue
  time, `running_concurrency` at run time).
- `key_arguments` for narrowing the conflict check to a subset of
  arguments.
- `reroute_on_concurrency_control` for choosing what happens to blocked
  invocations.
- A runnable demo, [`concurrency_demo`](https://github.com/pynenc/samples/tree/main/concurrency_demo),
  that visualises all of this on a pynmon timeline.

## Concurrency scopes

`ConcurrencyControlType` defines _what counts as a duplicate_:

| Scope       | What blocks what                                                                                       |
| ----------- | ------------------------------------------------------------------------------------------------------ |
| `DISABLED`  | Nothing. The default. Every invocation is independent.                                                 |
| `TASK`      | At most one invocation of _this task_ at a time. Arguments are ignored.                                |
| `ARGUMENTS` | At most one invocation per _full_ argument tuple. Same args = duplicate; any difference = independent. |
| `KEYS`      | At most one invocation per _subset_ of arguments. The subset is declared with `key_arguments=(...)`.   |

The same enum value goes on both flags. The flag decides _when_ the check
happens:

- **`registration_concurrency`** — checked when an invocation is enqueued.
  A duplicate is collapsed into a `ReusedInvocation` pointing to the
  existing one. The producer never gets a separate invocation back.
- **`running_concurrency`** — checked when a worker tries to execute an
  invocation. A blocked invocation transitions to
  `CONCURRENCY_CONTROLLED`; what happens next depends on
  `reroute_on_concurrency_control`.

`reroute_on_concurrency_control` (default `True`):

- `True` — the blocked invocation is requeued (`REROUTED`) and retried
  later, so it eventually runs once the slot frees up.
- `False` — the blocked invocation is discarded permanently
  (`CONCURRENCY_CONTROLLED_FINAL`); `inv.result` raises `KeyError`.

## Choosing a scope

```python
from pynenc import Pynenc
from pynenc.conf.config_task import ConcurrencyControlType as Mode

app = Pynenc()


# TASK — at most one nightly cleanup running, regardless of arguments.
@app.task(running_concurrency=Mode.TASK)
def nightly_cleanup(target: str) -> None: ...


# ARGUMENTS — same export request collapses; different exports run in parallel.
@app.task(registration_concurrency=Mode.ARGUMENTS)
def export_report(report_id: str, format: str) -> str: ...


# KEYS — serialise on account_id; the `op` argument is ignored when checking.
@app.task(
    running_concurrency=Mode.KEYS,
    key_arguments=("account_id",),
)
def call_external_api(account_id: str, op: str) -> str: ...
```

Pick the scope that matches the rule you actually need:

- _"Only one of these tasks ever runs at a time."_ → `TASK`.
- _"Don't run the same job twice."_ → `ARGUMENTS`.
- _"Don't run two jobs that share this key."_ → `KEYS` + `key_arguments`.

## Per-key concurrency: the common case

The most common production need is **per-tenant** or **per-account**
concurrency: parallelism _across_ tenants, serialisation _within_ each
tenant. That is exactly what `KEYS` is for.

```python
@app.task(
    running_concurrency=Mode.KEYS,
    key_arguments=("account_id",),
    reroute_on_concurrency_control=True,
)
def call_external_api(account_id: str, op: str, payload: dict) -> str:
    ...
```

With this:

- At most **one running invocation per `account_id`** at any time.
- Different `account_id` values run **in parallel** across all your
  workers.
- Blocked invocations are re-queued and complete once the slot opens.

Set `reroute_on_concurrency_control=False` instead when the right policy
is _"if a call for this account is already running, drop the new one"_.
Queue depth stays flat, no retry storm.

Add `registration_concurrency=Mode.KEYS` when duplicate enqueues for the
same key should collapse before they ever reach a worker — useful when an
event bus or scheduler may fire the same logical job many times.

## Worked example: `concurrency_demo`

The [`concurrency_demo`](https://github.com/pynenc/samples/tree/main/concurrency_demo)
sample runs four scenarios against a tiny FastAPI server that records a
`COLLISION` whenever two requests for the same account overlap. Run it
with:

```bash
git clone https://github.com/pynenc/samples
cd samples/concurrency_demo
uv sync
uv run python sample.py
```

All four scenarios on a single pynmon timeline:

```{image} ../_static/use_case_003-all-tests-timeline.png
:alt: All four concurrency scenarios on one pynmon invocation timeline
:align: center
```

### A — no concurrency control

Baseline. Eight worker threads pick up four invocations per account in
parallel; the provider records nine collisions.

```text
=== A. unsafe — no concurrency control ===
  12 enqueued -> 12 calls, 9 collisions, 1.42s
   X acme     calls=4  collisions=3
   X globex   calls=4  collisions=3
   X initech  calls=4  collisions=3
```

```{image} ../_static/use_case_003-tests-A-call-unsafe.png
:alt: Scenario A timeline — 12 invocations running in parallel, 9 collisions
:align: center
```

### B — `running_concurrency=KEYS`, `reroute=True`

Same 12 calls, zero collisions. The orchestrator refuses to start a second
invocation while one with the same `account_id` is running; blocked
invocations are rerouted until their slot opens.

```text
=== B. keyed — running_concurrency=KEYS, reroute=True ===
  12 enqueued -> 12 calls, 0 collisions, 2.14s
  OK acme     calls=4  collisions=0
  OK globex   calls=4  collisions=0
  OK initech  calls=4  collisions=0
```

```{image} ../_static/use_case_003-tests-B-call-keyed.png
:alt: Scenario B timeline — three serial lanes (one per account), parallel across accounts
:align: center
```

### C — `running_concurrency=KEYS`, `reroute=False`

Same guard, opposite policy. Blocked invocations land in
`CONCURRENCY_CONTROLLED_FINAL` and are dropped. Only the first call per
account ever reaches the provider.

```text
=== C. drop — running_concurrency=KEYS, reroute=False ===
  12 enqueued -> 3 calls (9 dropped), 0 collisions, 0.67s
  OK acme     calls=1  collisions=0
  OK globex   calls=1  collisions=0
  OK initech  calls=1  collisions=0
```

```{image} ../_static/use_case_003-tests-C-call-keyed-drop.png
:alt: Scenario C timeline — three running invocations, the rest dropped to CONCURRENCY_CONTROLLED_FINAL
:align: center
```

### D — `registration_concurrency=KEYS` + `running_concurrency=KEYS`

Dedupe at the door. 24 enqueues collapse to 3 invocations _before_ a
worker ever sees them, because every duplicate registration returns a
`ReusedInvocation` pointing at the first.

```text
=== D. dedupe — registration + running KEYS ===
  24 enqueued -> 3 calls (21 deduped), 0 collisions, 0.57s
  OK acme     calls=1  collisions=0
  OK globex   calls=1  collisions=0
  OK initech  calls=1  collisions=0
```

```{image} ../_static/use_case_003-tests-D-call-refresh-once.png
:alt: Scenario D timeline — 24 enqueues collapse to 3 invocations at registration time
:align: center
```

## Configuring defaults with `PynencBuilder`

Concurrency settings can also be set as app-wide defaults and overridden
per task:

```python
from pynenc import ConcurrencyControlType as Mode
from pynenc.builder import PynencBuilder

app = (
    PynencBuilder()
    .concurrency_control(
        running_concurrency=Mode.DISABLED,
        registration_concurrency=Mode.DISABLED,
    )
    .build()
)


@app.task  # inherits app defaults
def fast_path() -> None: ...


@app.task(running_concurrency=Mode.TASK)  # overrides only running
def heavy_singleton() -> None: ...
```

Builder defaults compose with all other configuration sources
(`pyproject.toml`, environment variables, YAML, JSON).

## Roadmap

The current primitive enforces _exactly one_ in-flight or registered
invocation per scope. Two extensions that build on the same orchestrator
machinery are on the roadmap:

- **Multi-slot concurrency** — _"up to N in flight per key"_.
- **Time-window rate limits** — _"at most M per minute per key"_.

## Related

- Sample: [`concurrency_demo`](https://github.com/pynenc/samples/tree/main/concurrency_demo)
  (FastAPI provider stand-in, four scenarios, pynmon-friendly).
- Reference: `pynenc.conf.config_task.ConcurrencyControlType`,
  `pynenc.conf.config_task.ConfigTask`.
- Related state: invocation status `CONCURRENCY_CONTROLLED`,
  `CONCURRENCY_CONTROLLED_FINAL`, `REROUTED`.
