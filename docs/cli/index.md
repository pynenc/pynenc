# Command Line Interface (CLI)

The Pynenc Command Line Interface (CLI) is a tool for managing your Pynenc application. So far just provides commands for starting runners and showing configuration, but more will be added in future releases.

## Mandatory App Option

Every command in the Pynenc CLI requires the `--app` option. This mandatory option specifies the application module and name, ensuring that the CLI is interacting with the correct Pynenc instance. Upon invocation, the CLI initializes the appropriate configuration classes and the runner based on the Pynenc configuration.

For more information on configuration, please refer to the {doc}`../configuration/index`.

## Basic Usage

The basic structure of a command in the Pynenc CLI is:

```bash
    $ python -m pynenc --app=<app_module> <command> [options]
```

Where `<app_module>` is the module path of your Pynenc application and `<command>` is one of the available commands.

## Available Commands

1. **runner**: Commands related to the runner.
2. **show_config**: Show the current configuration of the app or runner.

Each command might have its own set of options and arguments.

## show_config Command

To show the current configuration of your application:

```bash
    $ python -m pynenc --app=<app_module> show_config
```

This command displays the configuration settings of your Pynenc instance.

## runner Command

Use the `runner` command to manage the runner part of your Pynenc application.

```bash
    $ python -m pynenc --app=<app_module> runner [subcommand]
```

Subcommands for `runner` include:

- **start**: Start a runner.
- **show_config**: Show runner configuration.

## Runner Management

The `runner` command is used to control the runner component of your Pynenc application.

To start the runner:

```bash
    $ python -m pynenc --app=<app_module> runner start
```

This command initializes and starts the runner for your application. Note that using the default `DummyRunner` will result in an error, as shown below:

```text
    ValueError: DummyRunner cannot be started, use another runner
```

To use a functional runner, ensure that your application's configuration specifies a valid runner class. For example:

.. code-block:: bash

    $ PYNENC__RUNNER_CLS="ThreadRunner" python -m pynenc --app=<app_module> runner start

This command will start the `ThreadRunner`.

Stopping the Runner:

To stop the runner, you can interrupt the process (e.g., using `Ctrl+C`). Upon receiving the interrupt signal, the runner will attempt to shut down gracefully:

```text
    INFO: Received signal signum=2 Stopping runner loop...
    INFO: [runner: ThreadRunner] Stopping runner...
```
