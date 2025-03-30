import pathlib

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pynmon.app import get_pynenc_instance

router = APIRouter()

# Set up templates
base_dir = pathlib.Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(base_dir / "templates"))


@router.get("/", response_class=HTMLResponse, tags=["Dashboard"])
async def index(request: Request) -> HTMLResponse:
    """Dashboard home page."""
    pynenc = get_pynenc_instance()

    return templates.TemplateResponse(
        "index.html",
        {
            "request": request,
            "app_id": pynenc.app_id,
            "broker_type": pynenc.broker.__class__.__name__,
            "state_backend_type": pynenc.state_backend.__class__.__name__,
            "runner_type": pynenc.runner.__class__.__name__,
        },
    )
