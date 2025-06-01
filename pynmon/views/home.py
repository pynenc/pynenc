import logging
import pathlib

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pynmon.app import get_active_app, get_all_apps

logger = logging.getLogger("pynmon.views.home")
router = APIRouter()

# Set up templates
base_dir = pathlib.Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(base_dir / "templates"))


@router.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def index(request: Request) -> HTMLResponse:
    """Dashboard home page."""
    active_app = get_active_app()
    all_apps = get_all_apps()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_id": active_app.app_id,
            "all_apps": list(all_apps.keys()),
            "broker_type": active_app.broker.__class__.__name__,
            "state_backend_type": active_app.state_backend.__class__.__name__,
            "runner_type": active_app.runner.__class__.__name__,
        },
    )
