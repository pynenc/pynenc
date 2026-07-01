# Invocation Runtime Context

Pynenc uses thread-local runtime context to answer a simple question while a task
is running: "what execution environment am I currently inside?"

That runtime context is stored in `pynenc.context`, but the domain objects live
near the systems they describe. For example, `RunnerContext` lives under
`pynenc.runner`, while `pynenc.context` only stores and retrieves the active
runner context.

## Active Contexts

Pynenc currently tracks several runtime contexts:

| Context                        | What it represents                                       | Storage owner    |
| ------------------------------ | -------------------------------------------------------- | ---------------- |
| Runner context                 | The runner, process, and thread executing work           | `pynenc.context` |
| Distributed invocation context | The invocation currently running in this thread          | `pynenc.context` |
| Trigger event context          | The trigger event currently being evaluated or emitted   | `pynenc.context` |
| Current app                    | The active Pynenc app for core services and nested calls | `pynenc.context` |

`pynenc.context` is intentionally an ambient registry. It should not become the
home for domain classes; it should only expose get/set/swap helpers for active
runtime state.

## Invocation Kinds

Pynenc distinguishes the kind of invocation being executed:

| Invocation kind      | Created by                                          | Workflow meaning                                   |
| -------------------- | --------------------------------------------------- | -------------------------------------------------- |
| `WorkflowInvocation` | calling a `WorkflowTask` created by `@app.workflow` | Defines a workflow identity                        |
| `TaskInvocation`     | calling an ordinary `Task` created by `@app.task`   | Standalone, or a child inside an existing workflow |

This distinction is reconstructed from the resolved task role:
`WorkflowTask` creates `WorkflowInvocation`, while ordinary `Task` creates
`TaskInvocation`. State backends persist workflow membership, parent
references, and event origin; they do not need a separate invocation-kind field.

## Workflow Membership

A `WorkflowInvocation` always has a workflow identity. Its `workflow_id` is the
same value as the workflow-defining invocation id.

A `TaskInvocation` may have workflow membership when it is called from inside a
workflow. In that case it can use shared workflow APIs such as:

```python
current_task.wf.identity
current_task.wf.get_data("status")
current_task.wf.set_data("status", "payment_authorized")
```

A top-level ordinary task is standalone. It does not pretend to be a workflow,
and workflow accessors fail clearly when no workflow membership exists.

This is a runtime fact. Static typing can know that `@app.workflow` creates a
workflow-defining task, but it cannot know whether an ordinary `@app.task` call
will run standalone or inside an existing workflow.

## Root-Only Workflow Operations

Only workflow-defining invocations may orchestrate workflow replay:

```python
workflow_task.wf.root.uuid()
workflow_task.wf.root.random()
workflow_task.wf.root.utc_now()
workflow_task.wf.root.execute_task(child_task, *args, **kwargs)
```

Ordinary child tasks can share workflow data, but they cannot call root-only
operations. If they try, Pynenc raises `DeterministicOperationScopeError` with
debugging details such as the current task id, invocation id, workflow id, and
whether the invocation is workflow-defining.

The `task.wf` helper is always present. What changes at runtime is whether the
current invocation has workflow membership. Only `WorkflowTask.wf` exposes
`.root` statically.

This keeps the model clear:

- workflow roots make deterministic orchestration decisions
- ordinary tasks perform activity work
- shared workflow data remains available to all members of the workflow

## Distributed Coherence

Pynenc's invocation state machine already ensures that one invocation attempt is
owned by one runner. `PENDING` exists to acquire ownership before execution, and
ownership is required for the transition into `RUNNING`.

The workflow layer therefore does not add another distributed lock for
deterministic operations.

The coherent split is:

- local runtime state: the deterministic operation cursor on the active
  workflow-defining invocation object
- persisted state: deterministic values and child calls stored by workflow
  identity
- orchestrator state: retry count, rerouting, and invocation ownership

If a workflow invocation is retried on another runner, that runner reconstructs
the same invocation id and workflow identity, starts a fresh local cursor, and
replays the values already persisted for that workflow id.

## Execution Boundary

The current distributed invocation context is installed when a runner starts
running an invocation. It is restored or cleared when the invocation returns,
fails, or is rerouted.

```text
runner owns invocation
  -> install current app
  -> install runner context
  -> install current invocation context
  -> run task function
  -> restore previous contexts
```

For workflow replay behavior built on top of this invocation model, see
{doc}`../workflows/index`.
