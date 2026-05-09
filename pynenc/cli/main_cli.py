import argparse
import logging
import sys
import traceback
from typing import TYPE_CHECKING

from pynenc.cli.config_cli import add_config_subparser
from pynenc.cli.monitor_cli import add_monitor_subparser
from pynenc.cli.namespace import PynencCLINamespace
from pynenc.cli.runner_cli import add_runner_subparser
from pynenc.cli.status_cli import add_status_subparser
from pynenc.util.import_app import find_app_instance

if TYPE_CHECKING:
    from pynenc.app import Pynenc


def _set_auto_discovered_app_label(
    args: PynencCLINamespace, app_instance: "Pynenc"
) -> None:
    args.app_instance = app_instance
    if not args.app:
        args.app = f"auto-discovered app_id={app_instance.app_id}"


def _try_find_monitor_app() -> "Pynenc | None":
    """Try monitor auto-discovery without failing the command when no local app exists."""
    try:
        return find_app_instance(None)
    except ValueError:
        return None


def _resolve_app_for_command(args: PynencCLINamespace) -> None:
    """Attach app instance to args when the selected command requires one."""
    if not args.requires_app:
        return

    if args.command == "monitor" and not args.app:
        app_instance = _try_find_monitor_app()
        if app_instance is None:
            return
        _set_auto_discovered_app_label(args, app_instance)
        return

    app_instance = find_app_instance(args.app)
    _set_auto_discovered_app_label(args, app_instance)


def main() -> None:
    """
    Execute the Pynenc Command Line Interface.

    This function initializes and processes the command line arguments for the Pynenc application,
    sets up logging, and executes the appropriate subcommand function based on the user input.

    The CLI supports various subcommands for different functionalities, such as running tasks
    and configuring the application. The `--app` parameter is optional when the
    current directory contains exactly one importable `Pynenc()` instance; use it
    explicitly when more than one app exists or when running outside the app directory.

    The main steps include:
    - Parsing command line arguments with `argparse`.
    - Setting up logging based on verbosity.
    - Importing the application instance.
    - Executing the function associated with the chosen subcommand.

    Exceptions:
    - Various exceptions can be raised depending on the subcommands executed and the application's behavior.
    """
    parser = argparse.ArgumentParser(description="Pynenc Command Line Interface")
    parser.add_argument(
        "--app",
        help=(
            "Dotted path to the module containing your Pynenc() instance. "
            "Optional when the current directory contains exactly one local app. "
            "Examples: 'tasks.app' (loads tasks.py), "
            "'mypackage.tasks' (imports mypackage.tasks), "
            "'path/to/tasks.py' (loads file directly)"
        ),
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase output verbosity"
    )

    # Create subparsers for different CLI commands
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Add subparsers for different commands
    add_runner_subparser(subparsers)
    add_config_subparser(subparsers)
    add_monitor_subparser(subparsers)
    add_status_subparser(subparsers)

    # Parse the arguments into custom namespace PynencCLINamespace
    args = PynencCLINamespace()
    parser.parse_args(namespace=args)

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    try:
        _resolve_app_for_command(args)
        args.func(args)
    except ValueError as e:
        logging.error(f"Failed to load application: {e}")
        if args.verbose:
            traceback.print_exc()
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {type(e).__name__}: {e}")
        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
