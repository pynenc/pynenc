# Command Line Interface (CLI)

Reference for all Pynenc CLI commands and options.

## Basic Usage

```bash
pynenc <command> [options]
```

When the current directory contains exactly one importable Python file with a `Pynenc()` instance, the CLI can find it automatically. Use `--app` when a project has more than one app or when you run the command from another directory. It accepts:

| Format           | Example            | How it works                                                                |
| ---------------- | ------------------ | --------------------------------------------------------------------------- |
| `module.attr`    | `tasks.app`        | Loads `tasks.py` from the current directory and finds the `Pynenc` instance |
| `package.module` | `mypackage.tasks`  | Standard Python import via `importlib.import_module`                        |
| File path        | `path/to/tasks.py` | Loads the file directly                                                     |

**Common pattern** — create a `tasks.py` file:

```python
from pynenc import Pynenc

app = Pynenc()

@app.task
def add(x: int, y: int) -> int:
    return x + y
```

Then run with:

```bash
pynenc runner start
```

```{note}
The colon format (`module:variable`) is **not** supported. Use dot notation: `tasks.app` not `tasks:app`.
```

## Global Options

| Option            | Description                                                                                                |
| ----------------- | ---------------------------------------------------------------------------------------------------------- |
| `--app MODULE`    | Dotted path to module with `Pynenc()` instance. Optional when exactly one local app can be auto-discovered |
| `-v`, `--verbose` | Enable debug-level logging output                                                                          |

## Commands

### `runner start`

Start a task runner for the specified application.

```bash
pynenc runner start
```

If auto-discovery finds more than one app, specify the one to run:

```bash
pynenc --app myapp.tasks.app runner start
```

The runner type is determined by the application configuration (`runner_cls`). Using the default `DummyRunner` raises an error — configure a functional runner first.

To stop the runner, send `SIGINT` (`Ctrl+C`) or `SIGTERM`. The runner shuts down gracefully.

**Example with environment variable override**:

```bash
PYNENC__RUNNER_CLS=ThreadRunner pynenc runner start
```

### `runner show_config`

Display the runner configuration for the application.

```bash
pynenc runner show_config
```

### `show_config`

Display the full application configuration, including all components and their settings.

```bash
pynenc show_config
```

### `status render`

Render the built-in invocation status state machine. This command does not need
an app instance because it reads the status configuration from pynenc itself.

```bash
pynenc status render
pynenc status render --format svg --output docs/_static/invocation_state_machine.svg
```

### `monitor`

Start the Pynmon web monitoring interface. The `--app` option is optional — the monitor can auto-discover registered applications.

```bash
pynenc monitor [--host HOST] [--port PORT] [--log-level LEVEL]
```

| Option        | Default     | Description                                                |
| ------------- | ----------- | ---------------------------------------------------------- |
| `--host`      | `127.0.0.1` | Host to bind the server                                    |
| `--port`      | `8000`      | Port to bind the server                                    |
| `--log-level` | `info`      | Log level: `debug`, `info`, `warning`, `error`, `critical` |

**Examples**:

```bash
# Start monitor with auto-discovery
pynenc monitor

# Start with a specific app
pynenc --app myapp.tasks.app monitor --port 9000

# Start with debug logging
pynenc monitor --log-level debug
```

```{note}
The monitor requires the monitoring extras to be installed: `pip install pynenc[monitor]`.
It requires Python < 3.13 due to FastAPI/Pydantic v2 dependencies.
```

After starting, open `http://127.0.0.1:8000` in your browser to access the dashboard.

See {doc}`../monitoring/index` for details on what the monitoring UI provides.
