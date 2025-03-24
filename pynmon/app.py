from pathlib import Path
from typing import Optional

import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import HTMLResponse
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


def setup_routes() -> None:
    """Set up all route modules."""
    # Import view modules only when needed to avoid circular imports
    from pynmon.views import broker, calls, invocations, orchestrator, tasks

    # Register the routes
    app.include_router(broker.router)
    app.include_router(orchestrator.router)
    app.include_router(invocations.router)
    app.include_router(tasks.router)
    app.include_router(calls.router)


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
