# Monitoring with Pynmon

Reference for Pynmon, the built-in web monitoring interface for Pynenc.

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

## Dashboard Pages

### Home (`/`)

System overview showing application info, component types, and aggregate status counts for all invocations.

### Broker (`/broker/`)

Displays the broker implementation type and count of pending invocations. Includes a queue view showing queued invocation IDs.

| Endpoint              | Description                   |
| --------------------- | ----------------------------- |
| `GET /broker/`        | Broker overview               |
| `GET /broker/queue`   | Queue contents                |
| `GET /broker/refresh` | HTMX partial refresh          |
| `POST /broker/purge`  | Purge all pending invocations |

### Orchestrator (`/orchestrator/`)

Shows invocation status counts, blocking invocations, and active runner information.

| Endpoint                        | Description                              |
| ------------------------------- | ---------------------------------------- |
| `GET /orchestrator/`            | Orchestrator overview with status counts |
| `GET /orchestrator/refresh`     | HTMX partial refresh                     |
| `POST /orchestrator/auto-purge` | Auto-purge orchestrator data             |

### Runners (`/runners/`)

Lists active runners with heartbeat status, hostname, PID, creation time, and uptime. Individual runner detail pages show full context.

| Endpoint                               | Description                       |
| -------------------------------------- | --------------------------------- |
| `GET /runners/`                        | Active runners overview           |
| `GET /runners/refresh`                 | HTMX partial refresh              |
| `GET /runners/{runner_id}`             | Runner detail                     |
| `GET /runners/atomic-service/timeline` | Atomic service execution timeline |

### Tasks (`/tasks/`)

Browse all registered tasks with execution statistics. Task detail pages show all calls and invocations for a specific task.

| Endpoint                   | Description                            |
| -------------------------- | -------------------------------------- |
| `GET /tasks/`              | List all registered tasks              |
| `GET /tasks/refresh`       | HTMX partial refresh                   |
| `GET /tasks/{task_id_key}` | Task detail with calls and invocations |

### Invocations (`/invocations/`)

Paginated list of all invocations with status, task, and workflow filters. Includes a timeline visualization and individual invocation detail pages with full status history.

| Endpoint                            | Description                            |
| ----------------------------------- | -------------------------------------- |
| `GET /invocations/`                 | Paginated invocation list with filters |
| `GET /invocations/timeline`         | SVG timeline visualization             |
| `GET /invocations/table`            | HTMX partial table refresh             |
| `GET /invocations/{id}`             | Invocation detail with history         |
| `GET /invocations/{id}/history`     | Status history (JSON)                  |
| `GET /invocations/{id}/api`         | Invocation data (JSON)                 |
| `GET /invocations/{id}/family-tree` | Family tree SVG partial                |
| `POST /invocations/{id}/rerun`      | Re-run the same call                   |

### Calls (`/calls/`)

View details for specific task calls, including serialized arguments and associated invocations.

| Endpoint                   | Description                    |
| -------------------------- | ------------------------------ |
| `GET /calls/?call_id_key=` | Call detail by query parameter |
| `GET /calls/{call_id_key}` | Call detail by path            |

### Workflows (`/workflows/`)

Browse workflow types, view individual runs, and explore workflow hierarchies.

| Endpoint                            | Description            |
| ----------------------------------- | ---------------------- |
| `GET /workflows/`                   | List workflow types    |
| `GET /workflows/refresh`            | HTMX partial refresh   |
| `GET /workflows/runs`               | List all workflow runs |
| `GET /workflows/runs/refresh`       | HTMX partial refresh   |
| `GET /workflows/{type_key}`         | Workflow type detail   |
| `GET /workflows/{type_key}/refresh` | HTMX partial refresh   |

### State Backend (`/state-backend/`)

Displays state backend implementation type and provides a purge action.

| Endpoint                    | Description            |
| --------------------------- | ---------------------- |
| `GET /state-backend/`       | State backend overview |
| `POST /state-backend/purge` | Purge all state data   |

### Client Data Store (`/client-data-store/`)

Shows client data store configuration and provides a purge action.

| Endpoint                        | Description                |
| ------------------------------- | -------------------------- |
| `GET /client-data-store/`       | Client data store overview |
| `POST /client-data-store/purge` | Purge all cached data      |

### Log Explorer (`/log-explorer/`)

Paste Pynenc log lines for contextual analysis. The explorer parses runner contexts, invocation IDs, and task references, linking them to the appropriate detail pages and generating mini-timeline visualizations.

| Endpoint             | Description            |
| -------------------- | ---------------------- |
| `GET /log-explorer/` | Log explorer interface |

## Timeline Visualization

The timeline view (`/invocations/timeline`) renders an SVG-based visualization of invocation lifecycle across runners. It supports:

- **Time range selection**: Predefined ranges (1min, 5min, 1h, etc.) or custom date ranges
- **Filtering**: By task, workflow, or workflow type
- **Resolution control**: Adjustable time resolution for different zoom levels
- **Collapse external**: Option to collapse external runner invocations
- **Detail panel**: Click an invocation on the timeline to view status history and runner context
- **Family tree integration**: Cross-highlighting between timeline and family tree views

## Family Tree

The family tree (`/invocations/{id}/family-tree`) displays parent-child invocation hierarchies as an interactive SVG:

- Time-ordered grid layout showing invocation relationships
- Progressive expansion with "load more" for large trees
- Floating draggable panel with resize, collapse, and zoom controls
- Cross-highlighting with the timeline view

## App Switching

When multiple Pynenc applications share the same backend, use the app selector in the navigation to switch between them:

```
GET /switch-app/{app_id}
```

## Global Actions

| Endpoint      | Description                                                             |
| ------------- | ----------------------------------------------------------------------- |
| `POST /purge` | Purge all data (broker, orchestrator, state backend, client data store) |
