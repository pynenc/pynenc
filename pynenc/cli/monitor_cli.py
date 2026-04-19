import argparse
import importlib.util
import sys
from typing import TYPE_CHECKING

from pynenc.cli.namespace import PynencCLINamespace

if TYPE_CHECKING:
    from pynenc.app import AppInfo, Pynenc


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
    monitor_parser.add_argument(
        "--log-level",
        default=None,
        choices=["debug", "info", "warning", "error", "critical"],
        help="Log level for pynmon (default: info, or PYNMON_LOG_LEVEL env var)",
    )
    monitor_parser.set_defaults(func=start_monitor_command)


def start_monitor_command(args: PynencCLINamespace) -> None:
    """Execute the monitor command, starting the web interface."""
    # Check Python version compatibility
    if sys.version_info >= (3, 13):
        print("Error: Pynmon monitoring UI requires Python <3.13")
        print("Reason: FastAPI/Pydantic v2 dependencies do not support Python 3.13+")
        print("Note: Core pynenc functionality supports Python 3.13+")
        print("Solution: Use Python 3.11 or 3.12 for monitoring features")
        sys.exit(1)

    # Check if the monitor dependencies are installed
    if not _check_monitor_dependencies():
        print(
            "Monitor dependencies not installed. Please install with: "
            "pip install pynenc[monitor]"
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
    apps: dict[str, AppInfo] = get_all_available_apps(args)
    if not apps:
        print("Error: No Pynenc app instance available.")
        sys.exit(1)
    selected_app: Pynenc | None = None
    if hasattr(args, "app_instance") and args.app_instance:
        selected_app = args.app_instance

    print(
        f"Starting monitoring for app: {selected_app.app_id if selected_app else 'auto-discover'}"
    )
    try:
        start_monitor(
            apps=apps,
            selected_app=selected_app,
            host=args.host,
            port=args.port,
            log_level=getattr(args, "log_level", None),
        )
    except ValueError as exc:
        print(f"\nError: {exc}\n")
        print("Hint: make sure the app's state backend is accessible from")
        print("this environment (e.g. the SQLite DB exists).\n")
        print("  • Specify the app explicitly:  pynenc --app tasks monitor")
        print("  • Run with verbose mode for details:  pynenc -v monitor")
        sys.exit(1)


def _check_monitor_dependencies() -> bool:
    """Check if required monitoring dependencies are installed."""
    dependencies = ["fastapi", "jinja2", "uvicorn"]
    for dep in dependencies:
        if importlib.util.find_spec(dep) is None:
            return False
    return True


def _discover_apps_from_instance(app: "Pynenc") -> dict[str, "AppInfo"]:
    """
    Discover apps using an instantiated app's state backend.

    Best approach: the instance already has the correct config (e.g. custom
    sqlite_db_path set via the builder or config file).

    :param Pynenc app: The live application instance.
    :return: Dictionary mapping app_id to AppInfo.
    """
    from pynenc.app_info import AppInfo as _AppInfo

    try:
        info = app.state_backend.get_app_info()
        return {info.app_id: info}
    except (KeyError, ValueError):
        pass

    # App info not yet stored — build a minimal entry from the live instance
    info = _AppInfo.from_app(app)
    return {info.app_id: info}


def _discover_apps_from_config() -> dict[str, "AppInfo"]:
    """
    Discover apps using the default state backend configuration.

    Fallback approach when no ``--app`` is provided: relies on config from
    environment variables or ``pyproject.toml``.

    :return: Dictionary mapping app_id to AppInfo, or empty dict on failure.
    """
    from pynenc.app import Pynenc

    try:
        return Pynenc().state_backend.discover_app_infos()
    except (ValueError, KeyError):
        return {}


def get_all_available_apps(args: PynencCLINamespace) -> dict[str, "AppInfo"]:
    """
    Get all available apps in the current environment.

    Two strategies:

    1. **Instance-based** (``--app`` provided): Uses the instantiated app's
       state backend, which has the correct configuration (custom db path, etc.).
    2. **Config-based** (no ``--app``): Creates a default ``Pynenc()`` instance
       and queries its state backend using configuration from environment
       variables or ``pyproject.toml``.
    """
    if args.app_instance:
        return _discover_apps_from_instance(args.app_instance)
    return _discover_apps_from_config()
