import argparse
import logging

from ..util.import_app import find_app_instance
from .config_cli import add_config_subparser
from .namespace import PynencCLINamespace
from .runner_cli import add_runner_subparser


def main() -> None:
    parser = argparse.ArgumentParser(description="Pynenc Command Line Interface")
    parser.add_argument(
        "--app", help="Specify the application module and name", required=True
    )
    parser.add_argument(
        "-v", "--verbose", action="store_true", help="Increase output verbosity"
    )

    subparsers = parser.add_subparsers(dest="command", required=True)

    # Add subparsers for different commands
    add_runner_subparser(subparsers)
    add_config_subparser(subparsers)

    args = PynencCLINamespace()
    parser.parse_args(namespace=args)

    # Setup logging
    log_level = logging.DEBUG if args.verbose else logging.WARNING
    logging.basicConfig(level=log_level, format="%(levelname)s: %(message)s")

    # Import the app instance
    app_instance = find_app_instance(args.app)
    args.app_instance = app_instance  # Add app_instance to args

    # Execute the appropriate function based on the subcommand
    args.func(args)


if __name__ == "__main__":
    main()
