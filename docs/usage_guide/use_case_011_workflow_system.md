# Use Case 11: Workflow System

This use case shows how to build a durable multi-step workflow with normal
Pynenc code. It focuses on the practical shape of workflow code: workflow data,
root-only deterministic values, child task replay, sub-workflows, failure
recovery, and Pynmon inspection.

For the full conceptual model, see {doc}`../workflows/index`. For invocation
runtime details, see {doc}`../invocation/index`.

## Scenario

We will model order fulfillment:

1. validate an order
2. reserve inventory
3. charge payment
4. create a shipment in a sub-workflow
5. notify the customer
6. compensate by releasing inventory if payment fails
7. retry safely after a controlled transient failure

The runnable sample is in:

```text
samples/workflow_order_fulfillment
```

## App Setup

Use SQLite so the sample can run locally while still exercising durable state:

```python
from pynenc import PynencBuilder


app = (
    PynencBuilder()
    .app_id("workflow_order_fulfillment")
    .sqlite("workflow_order_fulfillment.db")
    .thread_runner(min_threads=1, max_threads=10)
    .build()
)
```

## Step Tasks

Keep side effects in child tasks. Child tasks can read and write shared workflow
data when they are called from a workflow, but deterministic IDs and child
orchestration should come from the workflow root.

```python
from typing import Any


class TransientFulfillmentError(RuntimeError):
    pass


@app.task
def validate_order(order: dict[str, Any], validated_at: str) -> dict[str, Any]:
    valid = bool(order.get("customer_id") and order.get("items"))
    validate_order.wf.set_data("validated_at", validated_at)
    return {"valid": valid, "total": order.get("total", 0)}


@app.task
def reserve_inventory(
    order_id: str,
    items: list[dict[str, Any]],
    reservation_id: str,
) -> dict[str, Any]:
    reserve_inventory.wf.set_data("reservation_id", reservation_id)
    return {"reserved": True, "reservation_id": reservation_id}


@app.task
def charge_payment(
    order_id: str,
    amount: float,
    payment_token: str,
    payment_id: str,
) -> dict[str, Any]:
    approved = payment_token != "decline"
    if approved:
        charge_payment.wf.set_data("payment_id", payment_id)
    return {"approved": approved, "payment_id": payment_id}


@app.task
def release_inventory(
    order_id: str,
    reservation_id: str,
    reason: str,
    release_id: str,
) -> dict[str, Any]:
    release_inventory.wf.set_data("inventory_release_id", release_id)
    return {
        "order_id": order_id,
        "reservation_id": reservation_id,
        "release_id": release_id,
        "reason": reason,
    }
```

Notice that these child tasks receive stable IDs and timestamps as arguments.
That keeps deterministic orchestration in the workflow-defining invocation while
still allowing child tasks to write workflow data.

## Sub-Workflow

Use `@app.workflow` when a nested part of the process deserves its own workflow
identity and workflow data:

```python
@app.task
def choose_carrier(order_id: str, address: dict[str, str]) -> dict[str, str]:
    service_level = "priority" if address.get("postal_code", "").startswith("8") else "standard"
    return {"carrier": "SwissPost", "service_level": service_level}


@app.task
def purchase_shipping_label(
    order_id: str,
    carrier: str,
    shipment_id: str,
    tracking_number: str,
) -> dict[str, str]:
    purchase_shipping_label.wf.set_data("shipment_id", shipment_id)
    purchase_shipping_label.wf.set_data("tracking_number", tracking_number)
    return {"shipment_id": shipment_id, "tracking_number": tracking_number}


@app.workflow
def shipping_workflow(order_id: str, address: dict[str, str]) -> dict[str, Any]:
    shipping_workflow.wf.set_data("order_id", order_id)
    shipping_workflow.wf.set_data("status", "shipping_started")

    carrier_inv = shipping_workflow.wf.root.execute_task(
        choose_carrier,
        order_id,
        address,
    )
    carrier = carrier_inv.result

    shipment_id = shipping_workflow.wf.root.uuid()
    tracking_number = (
        f"{carrier['carrier'][:3].upper()}-"
        f"{int(shipping_workflow.wf.root.random() * 1_000_000):06d}"
    )
    label_inv = shipping_workflow.wf.root.execute_task(
        purchase_shipping_label,
        order_id,
        carrier["carrier"],
        shipment_id,
        tracking_number,
    )
    label = label_inv.result

    shipping_workflow.wf.set_data("status", "shipping_label_created")
    return {
        "workflow_id": str(shipping_workflow.wf.identity.workflow_id),
        "parent_workflow_id": str(shipping_workflow.wf.identity.parent_workflow_id),
        "carrier": carrier,
        "label": label,
    }
```

The shipping workflow appears in Pynmon as its own workflow run, linked back to
the parent order workflow.

## Main Workflow

The main workflow coordinates the process with `wf.root.execute_task(...)`. That
API records child invocation ids at the workflow-run level, so retrying the
workflow can reuse child work that already completed.

```python
@app.workflow(
    retry_for=(TransientFulfillmentError,),
    max_retries=1,
)
def fulfill_order_workflow(order: dict[str, Any]) -> dict[str, Any]:
    order_id = order["order_id"]
    attempt = fulfill_order_workflow.wf.get_data("attempt_count", 0) + 1

    fulfill_order_workflow.wf.set_data("attempt_count", attempt)
    fulfill_order_workflow.wf.set_data("order_id", order_id)
    fulfill_order_workflow.wf.set_data("status", "started")

    validated_at = fulfill_order_workflow.wf.root.utc_now().isoformat()
    validation_inv = fulfill_order_workflow.wf.root.execute_task(
        validate_order,
        order,
        validated_at,
    )
    validation = validation_inv.result
    if not validation["valid"]:
        fulfill_order_workflow.wf.set_data("status", "validation_failed")
        return {"status": "validation_failed", "attempt_count": attempt}

    reservation_id = fulfill_order_workflow.wf.root.uuid()
    reservation_inv = fulfill_order_workflow.wf.root.execute_task(
        reserve_inventory,
        order_id,
        order["items"],
        reservation_id,
    )
    reservation = reservation_inv.result

    payment_id = fulfill_order_workflow.wf.root.uuid()
    payment_inv = fulfill_order_workflow.wf.root.execute_task(
        charge_payment,
        order_id,
        validation["total"],
        order["payment_token"],
        payment_id,
    )
    payment = payment_inv.result

    if not payment["approved"]:
        release_id = fulfill_order_workflow.wf.root.uuid()
        release_inv = fulfill_order_workflow.wf.root.execute_task(
            release_inventory,
            order_id,
            reservation["reservation_id"],
            "payment_declined",
            release_id,
        )
        fulfill_order_workflow.wf.set_data("status", "payment_failed")
        fulfill_order_workflow.wf.set_data("failure_reason", "payment_declined")
        return {
            "status": "payment_failed",
            "attempt_count": attempt,
            "release": release_inv.result,
        }

    shipment_inv = fulfill_order_workflow.wf.root.execute_task(
        shipping_workflow,
        order_id,
        order["shipping_address"],
    )
    shipment = shipment_inv.result

    if order.get("simulate_transient_after_shipping") and not fulfill_order_workflow.wf.get_data("transient_probe_raised"):
        fulfill_order_workflow.wf.set_data("transient_probe_raised", True)
        raise TransientFulfillmentError("controlled transient failure after label creation")

    fulfill_order_workflow.wf.set_data("status", "fulfilled")
    return {
        "workflow_id": str(fulfill_order_workflow.wf.identity.workflow_id),
        "status": "fulfilled",
        "attempt_count": attempt,
        "reservation_id": reservation["reservation_id"],
        "payment_id": payment["payment_id"],
        "shipment": shipment,
    }
```

The transient failure branch is intentional. The first attempt fails after the
shipping label is created. The retry runs the workflow function again, but the
validation, inventory, payment, and shipping child calls can resolve to already
recorded invocations.

## Running The Sample

From the sample directory:

```bash
cd samples/workflow_order_fulfillment
uv sync
uv run python sample.py
```

The script purges old state, starts a worker subprocess, runs the happy path,
the replay scenario, and the payment-failure scenario, then prints a compact
summary.

For an interactive run with Pynmon, use three terminals:

```bash
# Terminal 1: worker
uv run pynenc --app tasks.app runner start

# Terminal 2: enqueue scenarios
uv run python enqueue.py happy --purge
uv run python enqueue.py replay
uv run python enqueue.py payment_failure

# Terminal 3: monitoring UI
uv run pynenc --app tasks.app monitor
```

Open <http://127.0.0.1:8000>.

## What To Inspect In Pynmon

Useful views:

- `/workflows/` lists workflow types such as `fulfill_order_workflow` and
  `shipping_workflow`.
- `/workflows/runs` lists every workflow run.
- The main workflow invocation detail page shows retry status history.
- Timeline views outline workflow roots and sub-workflow roots.
- The family tree shows the parent workflow, child tasks, and the nested
  shipping workflow.
- `/invocations?workflow_id=<workflow-id>` filters the invocation table to one
  workflow run.

The replay scenario should show the same workflow id being retried, with child
steps before the failure point already completed.

## Checklist

Use this checklist when designing a workflow:

1. Put orchestration in an explicit `@app.workflow`.
2. Put side effects in child tasks called with `wf.root.execute_task(...)`.
3. Use `wf.root.uuid()`, `wf.root.random()`, and `wf.root.utc_now()` for values
   that must replay when the workflow invocation is retried.
4. Pass deterministic values into child tasks instead of generating them inside
   ordinary tasks.
5. Store business milestones in `wf.set_data(...)`.
6. Keep child task arguments stable across retry when you want replay.
7. Add distinct arguments when two child calls must produce separate
   invocations.
8. Use compensating tasks for business failures.
9. Use task retries for transient failures.
10. Inspect workflow runs and invocation family trees in Pynmon.

## Related Reading

- {doc}`../workflows/index` explains workflow identity, data, replay, and
  root-only orchestration.
- {doc}`../invocation/index` explains current invocation context and invocation
  kinds.
- {doc}`../monitoring/index` explains the Pynmon views used to inspect workflow
  runs.
