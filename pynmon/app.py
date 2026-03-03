"""
Pynmon monitoring application and server startup.

Note: Pynmon requires Python <3.13 due to FastAPI/Pydantic v2 dependency limitations.
Core pynenc functionality supports Python 3.11+.
"""

import sys

if sys.version_info >= (3, 13):
    raise RuntimeError(
        "The pynmon monitoring UI requires Python <3.13 due to FastAPI/Pydantic limitations. "
        "Core pynenc functionality supports Python 3.13+."
    )

import logging
import os
import traceback
from pathlib import Path

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.staticfiles import StaticFiles
from fastapi.templating import Jinja2Templates

from pynenc.app import AppInfo, Pynenc


# Standard library pattern: add NullHandler so that libraries using pynmon
# don't see "No handlers could be found for logger 'pynmon'" warnings.
# Actual handler/level configuration happens in configure_logging(), which is
# called only from the application entry point (start_monitor), not at import time.
# See: https://docs.python.org/3/howto/logging.html#configuring-logging-for-a-library
logging.getLogger("pynmon").addHandler(logging.NullHandler())

logger = logging.getLogger("pynmon")


def configure_logging(log_level: str = "INFO") -> None:
    """
    Configure pynmon logging for application use.

    Should be called once at server startup. Not called at import time so
    that embedding applications and tests can manage their own logging.
    Falls back to the PYNMON_LOG_LEVEL env var, then INFO.

    :param str log_level: Desired level ('debug', 'info', 'warning', 'error')
    """
    level_name = log_level.upper()
    level = getattr(logging, level_name, logging.INFO)

    pynmon_logger = logging.getLogger("pynmon")
    # Remove the NullHandler added at import time
    pynmon_logger.handlers = [
        h for h in pynmon_logger.handlers if not isinstance(h, logging.NullHandler)
    ]
    pynmon_logger.setLevel(level)
    pynmon_logger.propagate = False  # don't double-emit via root / uvicorn handler
    pynmon_logger.addHandler(_make_handler())


def _make_handler() -> logging.StreamHandler:
    """Build a coloured, timestamped stream handler for pynmon loggers."""
    try:
        from uvicorn.logging import DefaultFormatter

        fmt = "%(asctime)s %(levelprefix)s %(name)s: %(message)s"
        formatter = DefaultFormatter(
            fmt=fmt, datefmt="%Y-%m-%d %H:%M:%S", use_colors=True
        )
    except ImportError:
        formatter = logging.Formatter(  # type: ignore[assignment]
            "%(asctime)s %(levelname)-8s %(name)s: %(message)s",
            datefmt="%Y-%m-%d %H:%M:%S",
        )
    handler = logging.StreamHandler()
    handler.setFormatter(formatter)
    return handler


def _uvicorn_log_config() -> dict:
    """Return a uvicorn log config with timestamps and colors for uvicorn loggers.

    pynmon.* loggers are handled by the handler added above (not here) so they
    are not duplicated when uvicorn applies its dict config.
    """
    return {
        "version": 1,
        "disable_existing_loggers": False,
        "formatters": {
            "default": {
                "()": "uvicorn.logging.DefaultFormatter",
                "fmt": "%(asctime)s %(levelprefix)s %(message)s",
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "use_colors": True,
            },
            "access": {
                "()": "uvicorn.logging.AccessFormatter",
                "fmt": '%(asctime)s %(levelprefix)s %(client_addr)s - "%(request_line)s" %(status_code)s',
                "datefmt": "%Y-%m-%d %H:%M:%S",
                "use_colors": True,
            },
        },
        "handlers": {
            "default": {
                "formatter": "default",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stderr",
            },
            "access": {
                "formatter": "access",
                "class": "logging.StreamHandler",
                "stream": "ext://sys.stdout",
            },
        },
        "loggers": {
            "uvicorn": {"handlers": ["default"], "level": "INFO", "propagate": False},
            "uvicorn.error": {"level": "INFO"},
            "uvicorn.access": {
                "handlers": ["access"],
                "level": "INFO",
                "propagate": False,
            },
            # pynmon.* intentionally excluded — handled by the module-level
            # handler above which works in all execution contexts.
        },
    }


# Initialize FastAPI app
app = FastAPI(title="Pynenc Monitor")

# Set up static files
static_dir = Path(__file__).parent / "static"
app.mount("/static", StaticFiles(directory=str(static_dir)), name="static")

# Set up Jinja2 templates
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

# Global reference to the monitored Pynenc app instance
all_pynenc_instances: dict[str, Pynenc] = {}
pynenc_instance: Pynenc | None = None


# Global exception handler to catch and log all unhandled exceptions
@app.exception_handler(Exception)
async def global_exception_handler(request: Request, exc: Exception) -> JSONResponse:
    """Log all unhandled exceptions and return a 500 error."""
    logger.error(f"Unhandled exception in {request.method} {request.url}: {exc}")
    logger.error(f"Full traceback: {traceback.format_exc()}")

    # Return a user-friendly error response
    return JSONResponse(
        status_code=500,
        content={
            "error": "Internal Server Error",
            "message": f"An unexpected error occurred: {str(exc)}",
            "path": str(request.url.path),
        },
    )


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> HTMLResponse:
    """Root endpoint, shows the dashboard."""
    active_app = get_active_app()
    all_apps = get_all_apps()
    if not all_apps or not active_app:
        return templates.TemplateResponse(
            "critical_error.html",
            {
                "request": request,
                "title": "No App Configured",
                "message": "No Pynenc application is configured for monitoring.",
            },
        )

    return templates.TemplateResponse(
        "base.html",
        {
            "request": request,
            "title": "Pynenc Monitor Dashboard",
            "app_id": active_app.app_id,
            "all_apps": list(all_apps.keys()),
        },
    )


@app.get("/switch-app/{app_id}")
async def switch_app(app_id: str) -> RedirectResponse:
    """
    Switch the active app to the specified app_id.

    :param app_id: ID of the app to switch to
    :return: Redirect to the homepage
    :raises HTTPException: If the requested app doesn't exist
    """
    global pynenc_instance, all_pynenc_instances

    if app_id not in all_pynenc_instances:
        raise HTTPException(status_code=404, detail=f"App '{app_id}' not found.")

    pynenc_instance = all_pynenc_instances[app_id]
    logger.info(f"Switched to app: {app_id}")

    # Redirect to homepage
    return RedirectResponse("/", status_code=303)


@app.post("/purge")
async def purge_all() -> JSONResponse:
    """Purge all data from the app (broker, orchestrator, state backend, arg cache)."""
    if not pynenc_instance:
        raise HTTPException(
            status_code=500,
            detail="No Pynenc application is configured for monitoring.",
        )

    try:
        # Use the app's purge method which purges all components
        pynenc_instance.purge()
        return JSONResponse(
            {
                "success": True,
                "message": "All application data has been purged successfully.",
            }
        )
    except Exception as e:
        return JSONResponse(
            {"success": False, "message": f"Error while purging: {str(e)}"},
            status_code=500,
        )


def setup_routes() -> None:
    """Set up all route modules."""
    # Import view modules only when needed to avoid circular imports
    from pynmon.views import (
        broker,
        calls,
        client_data_store,
        family_tree,
        invocations,
        orchestrator,
        runners,
        state_backend,
        tasks,
        workflows,
    )

    # Register the routes
    app.include_router(broker.router)
    app.include_router(client_data_store.router)
    app.include_router(orchestrator.router)
    app.include_router(runners.router)
    # family_tree first: its /{id}/family-tree is more specific than /{id}
    app.include_router(family_tree.router)
    app.include_router(invocations.router)
    app.include_router(tasks.router)
    app.include_router(calls.router)
    app.include_router(state_backend.router)
    app.include_router(workflows.router)


def start_monitor(
    apps: dict[str, AppInfo],
    selected_app: Pynenc | None,
    host: str = "127.0.0.1",
    port: int = 8000,
    log_level: str | None = None,
) -> None:
    """
    Start the monitoring web server for a specific Pynenc app.

    :param apps: All available app instances to monitor
    :param selected_app: The initially active app instance
    :param host: Host to bind to
    :param port: Port to listen on
    :param log_level: Logging level ('debug', 'info', 'warning', 'error').
        Falls back to PYNMON_LOG_LEVEL env var, then 'info'.
    """
    resolved_level = log_level or os.environ.get("PYNMON_LOG_LEVEL") or "info"
    configure_logging(resolved_level)

    global pynenc_instance, all_pynenc_instances

    if not apps:
        raise ValueError("A Pynenc app instance must be provided")

    all_pynenc_instances = hydrate_app_instances(apps, selected_app)

    if not all_pynenc_instances:
        raise ValueError("No app instances could be initialized for monitoring")

    # Set the active app instance
    if selected_app and selected_app.app_id in all_pynenc_instances:
        pynenc_instance = selected_app
    else:
        pynenc_instance = next(iter(all_pynenc_instances.values()))

    logger.info(f"Primary app: {pynenc_instance.app_id}")
    logger.info(f"Available apps: {list(all_pynenc_instances.keys())}")

    setup_routes()

    print(f"Starting Pynenc Monitor at http://{host}:{port}")
    uvicorn.run(
        app,
        host=host,
        port=port,
        access_log=True,
        log_config=_uvicorn_log_config(),
    )


def get_pynenc_instance() -> Pynenc:
    """Get the Pynenc app instance being monitored."""
    global pynenc_instance

    if not pynenc_instance:
        raise HTTPException(
            status_code=500,
            detail="No Pynenc application is configured for monitoring.",
        )

    return pynenc_instance


def hydrate_app_instances(
    apps_info: dict[str, AppInfo], selected_app: Pynenc | None = None
) -> dict[str, Pynenc]:
    """
    Hydrate Pynenc app instances from AppInfo objects.

    Attempts to create or retrieve instances for each AppInfo entry,
    logging any errors encountered during the process.

    :param apps_info: Dictionary mapping app_id to AppInfo
    :param selected_app: Optional pre-loaded app instance to use
    :return: Dictionary of successfully hydrated app instances
    """
    instances = {}

    # Add the provided app instance first if available
    if selected_app:
        instances[selected_app.app_id] = selected_app
        logger.info(f"Using provided app instance: {selected_app.app_id}")

    # Try to hydrate each app from its info
    for app_id, app_info in apps_info.items():
        if app_id in instances:
            continue

        logger.info(f"Attempting to hydrate app: {app_id}")
        try:
            if app_instance := Pynenc.from_info(app_info):
                instances[app_id] = app_instance
                logger.info(f"Successfully hydrated app: {app_id}")
            else:
                logger.warning(f"Failed to hydrate app: {app_id}")
        except Exception as e:
            logger.error(f"Error hydrating app {app_id}: {e}", exc_info=True)

    return instances


def get_active_app() -> Pynenc:
    """
    Get the currently active Pynenc app instance.

    :return: Active Pynenc instance
    :raises HTTPException: If no app is configured
    """
    global pynenc_instance

    if not pynenc_instance:
        if all_pynenc_instances:
            # Auto-select the first available instance
            app_id, app = next(iter(all_pynenc_instances.items()))
            pynenc_instance = app
            logger.info(f"Auto-selected app: {app_id}")
        else:
            raise HTTPException(
                status_code=500,
                detail="No Pynenc application is configured for monitoring.",
            )

    return pynenc_instance


def get_all_apps() -> dict[str, Pynenc]:
    """Get all available Pynenc app instances."""
    global all_pynenc_instances
    return all_pynenc_instances
