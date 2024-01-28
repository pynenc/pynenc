import argparse

from pynenc.app import Pynenc
from pynenc.cli.config_cli import add_config_subparser
from pynenc.cli.namespace import PynencCLINamespace
from pynenc.runner import DummyRunner


def add_runner_subparser(subparsers: argparse._SubParsersAction) -> None:
    """
    Add subparsers for runner-related commands to the main argparse parser.

    This function sets up the CLI structure for commands related to the runner,
    including subcommands for starting a runner and showing its configuration.
    It integrates runner-specific commands into the broader CLI tool.

    :param argparse._SubParsersAction subparsers: The subparsers object from the main parser.
    """
    runner_parser = subparsers.add_parser("runner", help="Commands related to runner")
    runner_subparsers = runner_parser.add_subparsers(
        dest="runner_command", required=True
    )

    # Runner start command
    runner_start_parser = runner_subparsers.add_parser("start", help="Start a runner")
    runner_start_parser.set_defaults(func=start_runner_command)

    # Runner show_config command
    add_config_subparser(runner_subparsers)


def start_runner_command(args: PynencCLINamespace) -> None:
    """
    Execute the 'start' command for a Pynenc runner.

    This command initiates the runner associated with the Pynenc application instance.
    It ensures that the provided application instance is valid and not a DummyRunner,
    then starts the runner's execution. An error is raised if the runner is not suitable
    for starting (e.g., a DummyRunner).

    :param PynencCLINamespace args: The parsed CLI arguments, including the Pynenc application instance.
    """
    print(f"Starting runner for app: {args.app}")
    if not isinstance(args.app_instance, Pynenc):
        raise TypeError("app_instance must be an instance of Pynenc")
    app_instance = args.app_instance
    if isinstance(app_instance.runner, DummyRunner):
        raise ValueError("DummyRunner cannot be started, use another runner")
    app_instance.runner.run()
