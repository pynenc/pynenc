# Monitoring with Pynmon

Reference for Pynmon, the built-in web monitoring interface for Pynenc.

## Why Pynmon?

Setting up a distributed task system is the easy part — even easier with Pynenc. The hard part is **understanding what happened** when things go wrong. Which task called which? Where did it run? Why is it stuck? Pynmon gives you deep, real-time visibility into your distributed execution without any external tooling.

```{image} ../_static/pynmon_dashboard.png
:alt: Pynmon dashboard showing application overview, invocation status summary, component architecture, configuration, and quick navigation
:width: 100%
```

### Execution Timeline

See exactly what ran across every runner and worker, at every moment. Status transitions are color-coded — orange for pending, green for completed, blue for running — with connections between parent and child invocations. Click any invocation to inspect its full status history and the runner context that executed it.

```{image} ../_static/pynmon_timeline.png
:alt: Pynmon invocation timeline showing task execution across multiple runners with status transitions and parent-child connections
:width: 100%
```

### Family Tree

Navigate the full hierarchy of task calls as an interactive graph. When a grandparent task spawns parents that spawn children, the family tree displays the entire chain. Selecting a node cross-highlights it on the timeline, and vice versa — making it trivial to understand both the logical structure and the physical execution of complex workflows.

```{image} ../_static/pynmon_family_tree.png
:alt: Pynmon family tree showing invocation hierarchy with cross-highlighting to the execution timeline
:width: 100%
```

### Invocation Details

Every invocation has a detail page showing its status, arguments, result or exception, workflow membership, full status history with timestamps, runner context, and the family tree — all in one place.

```{image} ../_static/pynmon_inv_details.png
:alt: Pynmon invocation detail page showing status history, arguments, result, workflow information, and family tree
:width: 100%
```

### Log Explorer

Paste your Pynenc log lines and the Log Explorer augments them with full context. It parses runner contexts, invocation IDs, and task references from the logs, resolving each to its detail page. It generates a mini-timeline of all invocations mentioned in the logs — including their parents — and highlights runners and workers with direct links. Turn opaque log lines into a navigable map of your distributed execution.

```{image} ../_static/pynmon_log_explorer.png
:alt: Pynmon Log Explorer parsing log lines with augmented context, mini-timeline, and links to invocation and runner details
:width: 100%
```

## Overview

Pynmon provides real-time visibility into your distributed task execution through a web interface built with FastAPI, Jinja2, and HTMX. It connects to the same backends as your Pynenc application and displays live data about tasks, invocations, runners, and workflows.

## Installation

```bash
pip install pynenc[monitor]
```

Requires Python < 3.13 (FastAPI/Pydantic v2 dependency constraint).

## Starting the Monitor

```bash
# Auto-discover registered apps
pynenc monitor

# With a specific app
pynenc --app myapp.tasks.app monitor --host 0.0.0.0 --port 8000

# With debug logging
pynenc monitor --log-level debug
```

| Option        | Default     | Description         |
| ------------- | ----------- | ------------------- |
| `--host`      | `127.0.0.1` | Server bind address |
| `--port`      | `8000`      | Server bind port    |
| `--log-level` | `info`      | Logging verbosity   |

For a full list of HTTP endpoints, see {doc}`api`.

```{toctree}
:hidden:
:maxdepth: 1

api
```
