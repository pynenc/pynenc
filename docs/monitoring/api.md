# Pynmon API Reference

Complete list of HTTP endpoints exposed by the Pynmon monitoring server.

## Home

| Endpoint      | Description                               |
| ------------- | ----------------------------------------- |
| `GET /`       | Dashboard with system overview and status |
| `GET /health` | Lightweight health check (JSON)           |

## Broker (`/broker/`)

| Endpoint              | Description                   |
| --------------------- | ----------------------------- |
| `GET /broker/`        | Broker overview               |
| `GET /broker/queue`   | Queue contents                |
| `GET /broker/refresh` | HTMX partial refresh          |
| `POST /broker/purge`  | Purge all pending invocations |

## Orchestrator (`/orchestrator/`)

| Endpoint                        | Description                              |
| ------------------------------- | ---------------------------------------- |
| `GET /orchestrator/`            | Orchestrator overview with status counts |
| `GET /orchestrator/refresh`     | HTMX partial refresh                     |
| `POST /orchestrator/auto-purge` | Auto-purge orchestrator data             |

## Runners (`/runners/`)

| Endpoint                               | Description                       |
| -------------------------------------- | --------------------------------- |
| `GET /runners/`                        | Active runners overview           |
| `GET /runners/refresh`                 | HTMX partial refresh              |
| `GET /runners/{runner_id}`             | Runner detail                     |
| `GET /runners/atomic-service/timeline` | Atomic service execution timeline |

## Tasks (`/tasks/`)

| Endpoint                   | Description                            |
| -------------------------- | -------------------------------------- |
| `GET /tasks/`              | List all registered tasks              |
| `GET /tasks/refresh`       | HTMX partial refresh                   |
| `GET /tasks/{task_id_key}` | Task detail with calls and invocations |

## Invocations (`/invocations/`)

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

## Calls (`/calls/`)

| Endpoint                   | Description                    |
| -------------------------- | ------------------------------ |
| `GET /calls/?call_id_key=` | Call detail by query parameter |
| `GET /calls/{call_id_key}` | Call detail by path            |

## Workflows (`/workflows/`)

| Endpoint                            | Description            |
| ----------------------------------- | ---------------------- |
| `GET /workflows/`                   | List workflow types    |
| `GET /workflows/refresh`            | HTMX partial refresh   |
| `GET /workflows/runs`               | List all workflow runs |
| `GET /workflows/runs/refresh`       | HTMX partial refresh   |
| `GET /workflows/{type_key}`         | Workflow type detail   |
| `GET /workflows/{type_key}/refresh` | HTMX partial refresh   |

## State Backend (`/state-backend/`)

| Endpoint                    | Description            |
| --------------------------- | ---------------------- |
| `GET /state-backend/`       | State backend overview |
| `POST /state-backend/purge` | Purge all state data   |

## Client Data Store (`/client-data-store/`)

| Endpoint                        | Description                |
| ------------------------------- | -------------------------- |
| `GET /client-data-store/`       | Client data store overview |
| `POST /client-data-store/purge` | Purge all cached data      |

## Log Explorer (`/log-explorer/`)

| Endpoint             | Description            |
| -------------------- | ---------------------- |
| `GET /log-explorer/` | Log explorer interface |

## App Switching

| Endpoint                   | Description                        |
| -------------------------- | ---------------------------------- |
| `GET /switch-app/{app_id}` | Switch active app, redirect to `/` |

## Global Actions

| Endpoint      | Description                                                             |
| ------------- | ----------------------------------------------------------------------- |
| `POST /purge` | Purge all data (broker, orchestrator, state backend, client data store) |
