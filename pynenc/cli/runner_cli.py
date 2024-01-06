import argparse

from ..app import Pynenc
from .config_cli import add_config_subparser
from .namespace import PynencCLINamespace


def add_runner_subparser(subparsers: argparse._SubParsersAction) -> None:
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
    print(f"Starting runner for app: {args.app}")
    if not isinstance(args.app_instance, Pynenc):
        raise TypeError("app_instance must be an instance of Pynenc")
    app_instance = args.app_instance
    app_instance.runner.run()
