import pathlib

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates

from pynmon.app import get_pynenc_instance

router = APIRouter(prefix="/runners", tags=["Runners"])

# Set up templates
base_dir = pathlib.Path(__file__).parent.parent
templates = Jinja2Templates(directory=str(base_dir / "templates"))


@router.get("/", response_class=HTMLResponse)
async def runners_index(request: Request) -> HTMLResponse:
    """Runners overview page."""
    pynenc = get_pynenc_instance()

    runners_info = {
        "current_runner": pynenc.runner.__class__.__name__,
        "runners": [],
        "count": 0,
    }

    # Try to get runners if the method exists
    try:
        if hasattr(pynenc.broker, "get_runners"):
            runners = await pynenc.broker.get_runners()
            runners_info["runners"] = runners
            runners_info["count"] = len(runners)
        else:
            runners_info["not_implemented"] = True
    except Exception as e:
        runners_info["error"] = str(e)

    return templates.TemplateResponse(
        "runners.html",
        {
            "request": request,
            "runners_info": runners_info,
            "app_id": pynenc.app_id,
        },
    )


@router.get("/refresh", response_class=HTMLResponse)
async def refresh_runners(request: Request) -> HTMLResponse:
    """HTMX endpoint to refresh the runners list."""
    pynenc = get_pynenc_instance()

    runners = []
    error = None

    try:
        if hasattr(pynenc.broker, "get_runners"):
            runners = await pynenc.broker.get_runners()
        else:
            return templates.TemplateResponse(
                "components/not_implemented.html",
                {"request": request, "feature": "Runner listing"},
            )
    except Exception as e:
        error = str(e)

    return templates.TemplateResponse(
        "components/runner_list.html",
        {"request": request, "runners": runners, "error": error},
    )
