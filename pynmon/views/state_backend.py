import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/state-backend", tags=["state_backend"])
logger = logging.getLogger("pynmon.views.state_backend")


@router.get("/", response_class=HTMLResponse)
async def state_backend_view(request: Request) -> HTMLResponse:
    """Display state backend information and configuration."""
    start_time = time.time()
    logger.info("Starting state backend view")

    try:
        app = get_pynenc_instance()

        # Get basic state backend info
        state_backend_info = {
            "type": app.state_backend.__class__.__name__,
        }

        logger.info(
            f"Rendering state backend template in {time.time() - start_time:.2f}s"
        )
        return templates.TemplateResponse(
            "state_backend/overview.html",
            {
                "request": request,
                "title": "State Backend Monitor",
                "app_id": app.app_id,
                "state_backend_info": state_backend_info,
            },
        )
    except Exception as e:
        logger.error(f"Error in state_backend_view: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Error",
                "message": f"An error occurred in the state backend view: {str(e)}",
            },
            status_code=500,
        )


@router.post("/purge", response_class=JSONResponse)
async def purge_state_backend() -> JSONResponse:
    """Purge all state data."""
    app = get_pynenc_instance()

    try:
        # Purge the state backend
        app.state_backend.purge()
        return JSONResponse(
            {"success": True, "message": "State backend purged successfully"}
        )
    except Exception as e:
        logger.error(f"Error purging state backend: {str(e)}")
        return JSONResponse({"success": False, "message": f"Error: {str(e)}"}, 500)
