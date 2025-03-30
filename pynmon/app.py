from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.templating import Jinja2Templates

from pynenc.app import Pynenc

# Initialize FastAPI app
app = FastAPI(title="Pynenc Monitor")

# Set up Jinja2 templates
templates_dir = Path(__file__).parent / "templates"
templates = Jinja2Templates(directory=str(templates_dir))

# Global reference to the monitored Pynenc app instance
pynenc_instance: Optional[Pynenc] = None


@app.get("/", response_class=HTMLResponse)
async def root(request: Request) -> HTMLResponse:
    """Root endpoint, shows the dashboard."""
    if not pynenc_instance:
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
            "app_id": pynenc_instance.app_id,
        },
    )


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
    app_instance: Pynenc,
    host: str = "127.0.0.1",
    port: int = 8000,
) -> None:
    """
    Start the monitoring web server for a specific Pynenc app.

    :param app_instance: The Pynenc app instance to monitor
    :param host: Host to bind to
    :param port: Port to listen on
    """
    global pynenc_instance

    if not app_instance:
        raise ValueError("A Pynenc app instance must be provided")

    # Set the app instance
    pynenc_instance = app_instance

    # Make sure all routes are set up
    setup_routes()

    # Start the uvicorn server
    print(f"Starting Pynenc Monitor at http://{host}:{port}")
    print(f"Monitoring app: {pynenc_instance.app_id}")
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
