import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/arg-cache", tags=["arg_cache"])
logger = logging.getLogger("pynmon.views.arg_cache")


@router.get("/", response_class=HTMLResponse)
async def arg_cache_view(request: Request) -> HTMLResponse:
    """Display argument cache information and configuration."""
    start_time = time.time()
    logger.info("Starting arg cache view")

    try:
        app = get_pynenc_instance()

        # Get basic arg cache info
        arg_cache_info = {
            "type": app.arg_cache.__class__.__name__,
            "min_size_to_cache": app.arg_cache.conf.min_size_to_cache,
            "local_cache_size": app.arg_cache.conf.local_cache_size,
        }

        logger.info(f"Rendering arg cache template in {time.time() - start_time:.2f}s")
        return templates.TemplateResponse(
            "arg_cache/overview.html",
            {
                "request": request,
                "title": "Argument Cache Monitor",
                "app_id": app.app_id,
                "arg_cache_info": arg_cache_info,
            },
        )
    except Exception as e:
        logger.error(f"Error in arg_cache_view: {str(e)}")
        import traceback

        logger.error(traceback.format_exc())
        return templates.TemplateResponse(
            "shared/error.html",
            {
                "request": request,
                "title": "Error",
                "message": f"An error occurred in the arg cache view: {str(e)}",
            },
            status_code=500,
        )


@router.post("/purge", response_class=JSONResponse)
async def purge_arg_cache() -> JSONResponse:
    """Purge all argument cache data."""
    app = get_pynenc_instance()

    try:
        # Purge the arg cache
        app.arg_cache.purge()
        return JSONResponse(
            {"success": True, "message": "Argument cache purged successfully"}
        )
    except Exception as e:
        logger.error(f"Error purging argument cache: {str(e)}")
        return JSONResponse({"success": False, "message": f"Error: {str(e)}"}, 500)
