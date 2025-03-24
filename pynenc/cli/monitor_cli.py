import argparse
import importlib.util
import sys

from pynenc.cli.namespace import PynencCLINamespace


def add_monitor_subparser(subparsers: argparse._SubParsersAction) -> None:
    """Add the monitor subparser to the main pynenc CLI."""
    monitor_parser = subparsers.add_parser(
        "monitor", help="Start the web monitoring interface"
    )
    monitor_parser.add_argument(
        "--host",
        default="127.0.0.1",
        help="Host to bind the server (default: 127.0.0.1)",
    )
    monitor_parser.add_argument(
        "--port", type=int, default=8000, help="Port to bind the server (default: 8000)"
    )
    monitor_parser.set_defaults(func=start_monitor_command)


def start_monitor_command(args: PynencCLINamespace) -> None:
    """Execute the monitor command, starting the web interface."""
    # Check if the monitor dependencies are installed
    if not _check_monitor_dependencies():
        print(
            "Monitor dependencies not installed. Please install with: "
            "poetry install --with monitor"
        )
        sys.exit(1)

    # Import the app module only when dependencies are confirmed to exist
    try:
        from pynmon.app import start_monitor
    except ImportError:
        print(
            "Error: Monitoring features are not available. Please install pynenc with monitoring extras:"
        )
        print("pip install pynenc[monitor]")
        sys.exit(1)

    # Start the monitor with the provided app instance
    if not hasattr(args, "app_instance") or not args.app_instance:
        print("Error: No Pynenc app instance available.")
        sys.exit(1)

    print(f"Starting monitor for app: {args.app}")
    start_monitor(app_instance=args.app_instance, host=args.host, port=args.port)


def _check_monitor_dependencies() -> bool:
    """Check if required monitoring dependencies are installed."""
    dependencies = ["fastapi", "jinja2", "uvicorn"]
    for dep in dependencies:
        if importlib.util.find_spec(dep) is None:
            return False
    return True
