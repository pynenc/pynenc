import logging
import time

from fastapi import APIRouter, Request
from fastapi.responses import HTMLResponse, JSONResponse

from pynmon.app import get_pynenc_instance, templates

router = APIRouter(prefix="/client-data-store", tags=["client_data"])
logger = logging.getLogger("pynmon.views.client_data")


@router.get("/", response_class=HTMLResponse)
async def client_data_view(request: Request) -> HTMLResponse:
    """Display client data store information and configuration."""
    start_time = time.time()
    logger.info("Starting client data store view")

    try:
        app = get_pynenc_instance()

        # Get basic client data store info
        client_data_store_info = {
            "type": app.client_data_store.__class__.__name__,
            "min_size_to_cache": app.client_data_store.conf.min_size_to_cache,
            "local_cache_size": app.client_data_store.conf.local_cache_size,
            "max_size_to_cache": app.client_data_store.conf.max_size_to_cache,
            "warn_threshold": app.client_data_store.conf.warn_threshold,
            "compression_enabled": app.client_data_store.conf.compression_enabled,
            "disabled": app.client_data_store.conf.disable_client_data_store,
        }

        logger.info(
            f"Rendering client data store template in {time.time() - start_time:.2f}s"
        )
        return templates.TemplateResponse(
            request,
            "client_data_store/overview.html",
            context={
                "title": "Client Data Store Monitor",
                "app_id": app.app_id,
                "client_data_store_info": client_data_store_info,
            },
        )
    except Exception as e:
        logger.exception(f"Error in client_data_view: {str(e)}")
        return templates.TemplateResponse(
            request,
            "shared/error.html",
            context={
                "title": "Error",
                "message": f"An error occurred in the client data store view: {str(e)}",
            },
            status_code=500,
        )


@router.post("/purge", response_class=JSONResponse)
async def purge_client_data() -> JSONResponse:
    """Purge all argument cache data."""
    app = get_pynenc_instance()

    try:
        # Purge the arg cache
        app.client_data_store.purge()
        return JSONResponse(
            {"success": True, "message": "Client data store purged successfully"}
        )
    except Exception as e:
        logger.exception(f"Error purging client data store: {str(e)}")
        return JSONResponse({"success": False, "message": f"Error: {str(e)}"}, 500)
