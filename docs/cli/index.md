# Command Line Interface (CLI)

Reference for all Pynenc CLI commands and options.

## Basic Usage

```bash
pynenc --app <app_module> <command> [options]
```

The `--app` option specifies the Python module containing your `Pynenc` instance. It accepts a module path (e.g., `myapp.tasks.app`) or a file path (e.g., `myapp/tasks.py`).

## Global Options

| Option            | Description                                                               |
| ----------------- | ------------------------------------------------------------------------- |
| `--app MODULE`    | Application module or file path (required for `runner` and `show_config`) |
| `-v`, `--verbose` | Enable debug-level logging output                                         |

## Commands

### `runner start`

Start a task runner for the specified application.

```bash
pynenc --app myapp.tasks.app runner start
```

The runner type is determined by the application configuration (`runner_cls`). Using the default `DummyRunner` raises an error — configure a functional runner first.

To stop the runner, send `SIGINT` (`Ctrl+C`) or `SIGTERM`. The runner shuts down gracefully.

**Example with environment variable override**:

```bash
PYNENC__RUNNER_CLS=ThreadRunner pynenc --app myapp.tasks.app runner start
```

### `runner show_config`

Display the runner configuration for the application.

```bash
pynenc --app myapp.tasks.app runner show_config
```

### `show_config`

Display the full application configuration, including all components and their settings.

```bash
pynenc --app myapp.tasks.app show_config
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
