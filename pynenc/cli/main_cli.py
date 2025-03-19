import argparse
import logging
import sys

from pynenc.cli.config_cli import add_config_subparser
from pynenc.cli.namespace import PynencCLINamespace
from pynenc.cli.runner_cli import add_runner_subparser
from pynenc.util.import_app import find_app_instance


def main() -> None:
    """
    Execute the Pynenc Command Line Interface.

    This function initializes and processes the command line arguments for the Pynenc application,
    sets up logging, and executes the appropriate subcommand function based on the user input.

    The CLI supports various subcommands for different functionalities, such as running tasks
    and configuring the application. It requires the specification of an application module and name
    and optionally allows for increased output verbosity.

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
        help="Specify the application module (e.g., 'core.src.api.backtes') "
        "or file path (e.g., 'core/src/api/backtes.py')",
        required=True,
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase output verbosity"
    )

    # Create subparsers for different CLI commands
    subparsers = parser.add_subparsers(dest="command", required=True)

    # Add subparsers for different commands
    add_runner_subparser(subparsers)
    add_config_subparser(subparsers)

    # Parse the arguments into custom namespace PynencCLINamespace
    args = PynencCLINamespace()
    parser.parse_args(namespace=args)

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    try:
        app_instance = find_app_instance(args.app)
        args.app_instance = app_instance
        args.func(args)
    except ValueError as e:
        logging.error(f"Failed to load application: {str(e)}")
        sys.exit(1)
    except Exception as e:
        logging.error(f"An unexpected error occurred: {str(e)}")
        sys.exit(1)


if __name__ == "__main__":
    main()
