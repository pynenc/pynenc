# Workflows

Pynenc workflows add durable orchestration semantics to distributed tasks. Use
them when one business operation spans multiple task invocations and retrying
the operation must not duplicate work that already finished.

A workflow is explicit:

```python
@app.workflow
def fulfill_order_workflow(order: dict) -> dict:
    fulfill_order_workflow.wf.set_data("status", "started")
    tracking_id = fulfill_order_workflow.wf.root.uuid()
    ...
```

Ordinary `@app.task` functions are activities. They can do side effects, read
inputs, call external systems, and update shared workflow data when they are
running inside a workflow. They do not define workflows by themselves.

Use workflows for multi-step processes such as order fulfillment, customer
onboarding, document approval, report generation, or any process where retrying
the orchestrator should preserve completed child work.

## Workflow Tasks And Task Invocations

Pynenc separates workflow-defining invocations from ordinary task invocations:

| Concept              | Created by               | Meaning                                                  |
| -------------------- | ------------------------ | -------------------------------------------------------- |
| `WorkflowTask`       | `@app.workflow`          | A task that defines a workflow root or sub-workflow root |
| `Task`               | `@app.task`              | An ordinary activity task                                |
| `WorkflowInvocation` | calling a `WorkflowTask` | Defines a workflow identity                              |
| `TaskInvocation`     | calling a `Task`         | May run standalone or as a child inside a workflow       |

Calling an ordinary task at the top level creates a standalone invocation, not a
workflow. Calling a workflow task at the top level creates a workflow root.
Calling a workflow task from inside another workflow creates a sub-workflow with
its own workflow id and a parent workflow id.

## Workflow Identity

Every workflow-defining invocation owns a `WorkflowIdentity`:

```text
workflow_id   = workflow-defining invocation id
workflow_type = workflow task identifier
parent        = parent workflow id, if this is a sub-workflow
```

That identity is the durable boundary for workflow data, deterministic root
operations, and child invocation replay.

```python
@app.workflow
def shipping_workflow(order_id: str) -> dict:
    return {
        "workflow_id": str(shipping_workflow.wf.identity.workflow_id),
        "parent_workflow_id": str(shipping_workflow.wf.identity.parent_workflow_id),
    }
```

## Workflow Context

`task.wf` exposes workflow membership when the current invocation is inside a
workflow:

| Capability           | API                                                     | Available from                                     |
| -------------------- | ------------------------------------------------------- | -------------------------------------------------- |
| Identity             | `task.wf.identity`                                      | workflow roots and child tasks inside the workflow |
| Durable data         | `task.wf.set_data(...)`, `task.wf.get_data(...)`        | workflow roots and child tasks inside the workflow |
| Deterministic values | `workflow_task.wf.root.uuid()`, `random()`, `utc_now()` | workflow-defining invocations only                 |
| Durable child calls  | `workflow_task.wf.root.execute_task(...)`               | workflow-defining invocations only                 |

The split is intentional. Workflow data is a shared distributed datastore for
the workflow run, so child tasks may read and write it. Orchestration decisions
belong to the workflow root, so deterministic values and child task replay live
under `wf.root`.

Trying to use workflow APIs from a standalone ordinary task fails clearly. Trying
to call root-only operations from an ordinary child task raises
`DeterministicOperationScopeError`.

## Replay Model

If a workflow task is retried, Pynenc runs the Python function again. The
workflow root APIs provide the durable pieces needed to make that retry safe:

- `wf.root.uuid()`, `wf.root.random()`, and `wf.root.utc_now()` replay values
  stored for the workflow-defining invocation id.
- `wf.root.execute_task(...)` returns a previously recorded child invocation
  when the same workflow reaches the same child call with the same arguments.
- `wf.get_data(...)` and `wf.set_data(...)` preserve workflow milestones and
  decisions across attempts.

This means a retry can move past work that already succeeded:

```python
shipment_id = fulfill_order_workflow.wf.root.uuid()

shipment_inv = fulfill_order_workflow.wf.root.execute_task(
    create_shipment,
    order_id,
    shipment_id,
)
shipment = shipment_inv.result

if order.get("simulate_transient_after_shipping") and not fulfill_order_workflow.wf.get_data("probe_raised"):
    fulfill_order_workflow.wf.set_data("probe_raised", True)
    raise TransientFulfillmentError("fail once after shipment")
```

On retry, the workflow function runs from the top, but earlier child calls can
resolve to already recorded invocations. Only the work after the failure point
has to run.

```{note}
Child replay is keyed by task and arguments. Two calls to the same child task
with identical arguments represent the same recorded child invocation. Add a
distinct argument, such as a line id or step name, when you need two separate
side effects.
```

## Deterministic Operations

Workflow root code should not call `uuid.uuid4()`, `random.random()`, or
`datetime.now()` for values that must replay after retry. Use `wf.root` instead:

```python
token = generate_invoice.wf.root.uuid()
sample = generate_invoice.wf.root.random()
created_at = generate_invoice.wf.root.utc_now()
```

Those values are stored in workflow state under the workflow id. Because
`workflow_id` is the invocation id of the workflow-defining task, a retry of the
same workflow invocation replays the same deterministic sequence. A new
workflow invocation gets a new workflow id and therefore a new sequence.

```{important}
Determinism depends on operation order. If a retry takes a different branch
before the same deterministic calls are replayed, it may intentionally generate
new values for the new branch. Store business decisions in workflow data when
they must remain fixed across branches.
```

## Child Tasks

Keep side effects in ordinary tasks and pass deterministic values from the
workflow root:

```python
@app.task
def charge_payment(order_id: str, amount: float, payment_id: str) -> dict:
    charge_payment.wf.set_data("payment_id", payment_id)
    return {"payment_id": payment_id, "approved": True}


@app.workflow
def fulfill_order_workflow(order: dict) -> dict:
    payment_id = fulfill_order_workflow.wf.root.uuid()
    payment_inv = fulfill_order_workflow.wf.root.execute_task(
        charge_payment,
        order["order_id"],
        order["total"],
        payment_id,
    )
    return payment_inv.result
```

The child task can update shared workflow data, but it cannot create workflow
root deterministic values or orchestrate other children through `wf.root`.

## Sub-Workflows

Use another `@app.workflow` when a nested part of the process deserves its own
workflow identity and lifecycle:

```python
@app.workflow
def shipping_workflow(order_id: str) -> dict:
    shipping_workflow.wf.set_data("status", "shipping_started")
    ...


@app.workflow
def fulfill_order_workflow(order: dict) -> dict:
    shipment_inv = fulfill_order_workflow.wf.root.execute_task(
        shipping_workflow,
        order["order_id"],
    )
    return shipment_inv.result
```

A sub-workflow has its own workflow data and deterministic root sequence. Pass
values explicitly when it needs parent context.

## Workflow Data

Workflow data is a durable key-value store scoped to one workflow identity:

```python
fulfill_order_workflow.wf.set_data("order_id", order_id)
fulfill_order_workflow.wf.set_data("status", "payment_authorized")

status = fulfill_order_workflow.wf.get_data("status", "unknown")
```

Use workflow data for milestones and decisions that should survive retry:

- `status`
- `attempt_count`
- `reservation_id`
- `payment_id`
- `shipment_id`
- `failure_reason`

Avoid using workflow data as an unbounded event log. Store large payloads
through normal task results or an application data store.

## Failure Recovery

Workflows usually handle business failures and infrastructure failures
differently.

For business failures, return a terminal business status and run compensating
tasks explicitly:

```python
if not payment["approved"]:
    release_inv = fulfill_order_workflow.wf.root.execute_task(
        release_inventory,
        order_id,
        reservation_id,
        "payment_declined",
    )
    fulfill_order_workflow.wf.set_data("status", "payment_failed")
    return {"status": "payment_failed", "release": release_inv.result}
```

For transient infrastructure failures, raise a retriable exception from a
workflow configured with `retry_for` and `max_retries`. The same workflow
invocation is retried, so the workflow identity and workflow data remain
attached.

Pynenc does not currently expose a production pause/resume workflow API. The
`WorkflowPauseError` type exists internally, but runner support for user-facing
pause/resume is not implemented yet.

## Monitoring

Pynmon reads the same durable state backend as the worker:

```bash
uv run pynenc --app tasks.app monitor
```

Useful workflow views:

- `/workflows/` lists workflow types and run counts.
- `/workflows/runs` lists every workflow run.
- `/workflows/{workflow_type}` lists runs for one workflow type.
- `/invocations?workflow_id=<workflow-id>` filters invocations to one workflow.
- Invocation detail pages show workflow membership, status history, arguments,
  result, and family tree.

Workflow-defining invocations are visually outlined in timeline views, so root
workflows and sub-workflow roots stand out from ordinary child tasks.

Pynmon's value for workflows is causal debugging: which workflow run did this
invocation belong to, which child task did it call, did the workflow retry, and
which child calls were already completed before retry?

## Tutorial

For a practical order-fulfillment walkthrough, see
{doc}`../usage_guide/use_case_011_workflow_system`.

For the runnable sample, see:

```text
samples/workflow_order_fulfillment
```
