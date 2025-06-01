import logging
from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse, RedirectResponse
from fastapi.templating import Jinja2Templates

from pynenc.app import AppInfo, Pynenc

# Configure logging
logger = logging.getLogger("pynmon")
handler = logging.StreamHandler()
handler.setFormatter(
    logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")
)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

# Initialize FastAPI app
app = FastAPI(title="Pynenc Monitor")

# Set up Jinja2 templates
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

# Global reference to the monitored Pynenc app instance
all_pynenc_instances: dict[str, Pynenc] = {}
pynenc_instance: Optional[Pynenc] = None


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
        arg_cache,
        broker,
        calls,
        invocations,
        orchestrator,
        state_backend,
        tasks,
    )

    # Register the routes
    app.include_router(broker.router)
    app.include_router(orchestrator.router)
    app.include_router(invocations.router)
    app.include_router(tasks.router)
    app.include_router(calls.router)
    app.include_router(state_backend.router)
    app.include_router(arg_cache.router)


def start_monitor(
    apps: dict[str, AppInfo],
    selected_app: Pynenc | None,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """
    Start the monitoring web server for a specific Pynenc app.

    :param app_instance: The Pynenc app instance to monitor
    :param host: Host to bind to
    :param port: Port to listen on
    """

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
    uvicorn.run(app, host=host, port=port)


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
    apps_info: dict[str, AppInfo], selected_app: Optional[Pynenc] = None
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
